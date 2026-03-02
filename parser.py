"""
RedM Server List Parser

1. Fetches full server list from cfx.re streaming API (protobuf frames)
2. Filters RedM servers (gamename=rdr3)
3. Fetches each server's details (with resources) via single-server JSON API
   — works in batches of ~60, with cooldown pauses to avoid Akamai WAF blocks
4. Saves progress incrementally, supports resuming interrupted runs
5. Aggregates resource usage statistics and saves to JSON

Rate limits (Akamai CDN):
  - ~80 requests before 403 block
  - Block lasts ~5-10 minutes
  - Strategy: batches of 60 + 5 min cooldown between batches

Multi-worker mode (for GitHub Actions):
  python parser.py prepare -w 8       # split into 8 chunks
  python parser.py fetch -c chunk.json -o result.json  # fetch one chunk
  python parser.py merge              # merge all results
  python parser.py                    # original single-process mode
"""

import argparse
import glob as glob_mod
import json
import math
import os
import re
import struct
import time
from datetime import datetime, timezone

import requests

STREAM_URL = "https://servers-frontend.fivem.net/api/servers/streamRedir/"
SINGLE_URL = "https://servers-frontend.fivem.net/api/servers/single/{}"
OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "resources.json")
PROGRESS_FILE = os.path.join(OUTPUT_DIR, "progress.json")
CHUNKS_DIR = os.path.join(OUTPUT_DIR, "chunks")
RESULTS_DIR = os.path.join(OUTPUT_DIR, "results")

COLOR_CODE_RE = re.compile(r"\^[0-9]")

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Accept": "*/*",
    "Origin": "https://servers.redm.net",
    "Referer": "https://servers.redm.net/",
}

BATCH_SIZE = 75       # requests per batch (close to ~80 limit, safe with separate IPs)
BATCH_COOLDOWN = 300  # seconds (5 min) between batches


# ─── Protobuf decoder (minimal) ─────────────────────────────────────────


def _decode_varint(data: bytes, pos: int) -> tuple[int, int]:
    result = 0
    shift = 0
    while True:
        b = data[pos]
        result |= (b & 0x7F) << shift
        pos += 1
        if not (b & 0x80):
            break
        shift += 7
    return result, pos


def _skip_field(data: bytes, pos: int, wire_type: int) -> int:
    if wire_type == 0:
        _, pos = _decode_varint(data, pos)
    elif wire_type == 1:
        pos += 8
    elif wire_type == 2:
        length, pos = _decode_varint(data, pos)
        pos += length
    elif wire_type == 5:
        pos += 4
    else:
        raise ValueError(f"Unknown wire type {wire_type}")
    return pos


def _decode_string(data: bytes, pos: int) -> tuple[str, int]:
    length, pos = _decode_varint(data, pos)
    s = data[pos : pos + length].decode("utf-8", errors="replace")
    return s, pos + length


def _decode_server_data_lite(data: bytes, pos: int, end: int) -> dict:
    """Decode ServerData — only extract fields needed for filtering."""
    result = {"hostname": "", "clients": 0, "sv_maxclients": 0, "gamename": "", "mapname": ""}
    while pos < end:
        tag, pos = _decode_varint(data, pos)
        fn = tag >> 3
        wt = tag & 0x7
        if fn == 1 and wt == 0:
            val, pos = _decode_varint(data, pos)
            result["sv_maxclients"] = val
        elif fn == 2 and wt == 0:
            val, pos = _decode_varint(data, pos)
            result["clients"] = val
        elif fn == 4 and wt == 2:
            val, pos = _decode_string(data, pos)
            result["hostname"] = val
        elif fn == 6 and wt == 2:
            val, pos = _decode_string(data, pos)
            result["mapname"] = val
        elif fn == 12 and wt == 2:
            length, pos = _decode_varint(data, pos)
            entry_end = pos + length
            key = ""
            value = ""
            while pos < entry_end:
                etag, pos = _decode_varint(data, pos)
                efn = etag >> 3
                ewt = etag & 0x7
                if efn == 1 and ewt == 2:
                    key, pos = _decode_string(data, pos)
                elif efn == 2 and ewt == 2:
                    value, pos = _decode_string(data, pos)
                else:
                    pos = _skip_field(data, pos, ewt)
            if key == "gamename":
                result["gamename"] = value
        else:
            pos = _skip_field(data, pos, wt)
    return result


def _decode_server_frame(data: bytes, pos: int, end: int) -> dict:
    endpoint = ""
    server_data = {}
    while pos < end:
        tag, pos = _decode_varint(data, pos)
        fn = tag >> 3
        wt = tag & 0x7
        if fn == 1 and wt == 2:
            endpoint, pos = _decode_string(data, pos)
        elif fn == 2 and wt == 2:
            length, pos = _decode_varint(data, pos)
            server_data = _decode_server_data_lite(data, pos, pos + length)
            pos += length
        else:
            pos = _skip_field(data, pos, wt)
    return {"endpoint": endpoint, **server_data}


def read_frames(raw: bytes) -> list[dict]:
    servers = []
    pos = 0
    total = len(raw)
    while pos + 4 <= total:
        frame_len = struct.unpack_from("<I", raw, pos)[0]
        pos += 4
        if frame_len == 0 or pos + frame_len > total or frame_len > 65535:
            break
        try:
            servers.append(_decode_server_frame(raw, pos, pos + frame_len))
        except Exception:
            pass
        pos += frame_len
    return servers


# ─── Single server fetcher ───────────────────────────────────────────────

session = requests.Session()
session.headers.update(HEADERS)


def fetch_single_server(endpoint: str) -> tuple[dict | None, str]:
    """Fetch full server details. Returns (data, error_reason)."""
    for attempt in range(3):
        try:
            resp = session.get(SINGLE_URL.format(endpoint), timeout=15)
            if resp.status_code == 404:
                return None, "404"
            if resp.status_code in (403, 429):
                return None, "rate_limit"
            resp.raise_for_status()
            return resp.json(), ""
        except requests.exceptions.Timeout:
            if attempt < 2:
                time.sleep(1)
        except Exception:
            if attempt < 2:
                time.sleep(0.5 * (attempt + 1))
    return None, "failed"


# ─── Progress management ────────────────────────────────────────────────


def load_progress() -> dict:
    """Load progress from previous interrupted run."""
    if os.path.exists(PROGRESS_FILE):
        with open(PROGRESS_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}


def save_progress(done_endpoints: set, server_details: dict):
    """Save progress: which endpoints are done, and their fetched data."""
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    with open(PROGRESS_FILE, "w", encoding="utf-8") as f:
        json.dump(
            {"done": list(done_endpoints), "details": server_details},
            f,
            ensure_ascii=False,
        )


def clear_progress():
    if os.path.exists(PROGRESS_FILE):
        os.remove(PROGRESS_FILE)


# ─── Main logic ──────────────────────────────────────────────────────────


def strip_colors(text: str) -> str:
    return COLOR_CODE_RE.sub("", text)


def wait_for_unblock():
    """Wait until API responds with 200 again."""
    print("  Checking if API is available...")
    for attempt in range(20):
        try:
            r = session.get(SINGLE_URL.format("45o3a9"), timeout=10)
            if r.status_code == 200:
                print("  API available!")
                return
        except Exception:
            pass
        wait = min(30, 10 + attempt * 5)
        print(f"  Still blocked, waiting {wait}s... ({attempt+1})")
        time.sleep(wait)
    print("  Warning: API may still be blocked, proceeding anyway...")


def fetch_stream_with_retry(max_retries=5) -> bytes:
    for attempt in range(max_retries):
        try:
            resp = requests.get(STREAM_URL, headers=HEADERS, timeout=120)
            if resp.status_code in (403, 429):
                wait = 60 * (attempt + 1)
                print(f"  Got {resp.status_code}, waiting {wait}s before retry ({attempt+1}/{max_retries})...")
                time.sleep(wait)
                continue
            resp.raise_for_status()
            return resp.content
        except requests.exceptions.ConnectionError:
            wait = 10 * (attempt + 1)
            print(f"  Connection error, waiting {wait}s before retry ({attempt+1}/{max_retries})...")
            time.sleep(wait)
    raise RuntimeError("Failed to fetch server stream after retries. Try again later.")


def aggregate_and_save(redm_servers: list[dict], server_details: dict):
    """Aggregate resource stats from fetched server details and save output."""
    resources = {}
    servers_list = []
    total_with_resources = 0

    for endpoint, detail in server_details.items():
        data = detail.get("Data", {})
        res_list = data.get("resources", [])
        if not res_list:
            continue

        clean_resources = [r.strip() for r in res_list if isinstance(r, str) and r.strip()]
        if not clean_resources:
            continue

        total_with_resources += 1
        hostname = strip_colors(data.get("hostname", "Unknown"))
        clients = data.get("clients", 0)
        max_clients = data.get("svMaxclients") or data.get("sv_maxclients", 0)

        server_entry = {
            "id": endpoint,
            "name": hostname,
            "players": clients,
            "max_players": max_clients,
        }

        # Add to servers list (with resource names)
        servers_list.append({**server_entry, "resources": clean_resources})

        for res_name in clean_resources:
            if res_name not in resources:
                resources[res_name] = {"count": 0, "servers": []}
            resources[res_name]["count"] += 1
            resources[res_name]["servers"].append(server_entry)

    for res in resources.values():
        res["servers"].sort(key=lambda s: s["players"], reverse=True)

    servers_list.sort(key=lambda s: s["players"], reverse=True)

    result = {
        "parsed_at": datetime.now(timezone.utc).isoformat(),
        "total_servers": len(redm_servers),
        "total_servers_with_resources": total_with_resources,
        "total_resources": len(resources),
        "resources": resources,
        "servers": servers_list,
    }

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False)

    return total_with_resources, len(resources)


def main():
    # Step 1: Get server list from stream
    print("Step 1: Fetching server list from stream...")
    raw = fetch_stream_with_retry()
    print(f"  Downloaded {len(raw):,} bytes")

    all_servers = read_frames(raw)
    print(f"  Total servers in stream: {len(all_servers)}")

    redm_servers = [s for s in all_servers if s.get("gamename") == "rdr3"]
    print(f"  RedM servers: {len(redm_servers)}")

    # Step 2: Load progress from previous run (if any)
    progress = load_progress()
    done_endpoints = set(progress.get("done", []))
    server_details = progress.get("details", {})

    remaining = [s for s in redm_servers if s["endpoint"] not in done_endpoints]

    if done_endpoints:
        print(f"\n  Resuming: {len(done_endpoints)} already done, {len(remaining)} remaining")

    # Step 3: Fetch details in batches
    total_batches = (len(remaining) + BATCH_SIZE - 1) // BATCH_SIZE
    print(f"\nStep 2: Fetching details in {total_batches} batches of {BATCH_SIZE}")
    print(f"  (cooldown {BATCH_COOLDOWN}s between batches to avoid rate limits)")

    batch_num = 0
    for batch_start in range(0, len(remaining), BATCH_SIZE):
        batch = remaining[batch_start : batch_start + BATCH_SIZE]
        batch_num += 1

        # Wait for API to be available before each batch (except first if fresh start)
        if batch_start > 0:
            print(f"\n  Cooldown: waiting {BATCH_COOLDOWN}s before batch {batch_num}/{total_batches}...")
            time.sleep(BATCH_COOLDOWN)
            wait_for_unblock()

        print(f"\n  Batch {batch_num}/{total_batches}: fetching {len(batch)} servers...")
        batch_ok = 0
        batch_err = 0
        hit_rate_limit = False

        for i, srv in enumerate(batch):
            ep = srv["endpoint"]
            detail, reason = fetch_single_server(ep)

            if reason == "rate_limit":
                hit_rate_limit = True
                batch_err += 1
                print(f"\n  Rate limit hit at request {i+1} in batch — stopping batch early")
                break

            if detail:
                server_details[ep] = detail
                done_endpoints.add(ep)
                batch_ok += 1
            else:
                done_endpoints.add(ep)  # mark as done even if 404
                batch_err += 1

            if (i + 1) % 10 == 0:
                print(f"    [{i+1}/{len(batch)}] ok={batch_ok} err={batch_err}", end="\r")

        print(f"    Batch {batch_num} done: ok={batch_ok} err={batch_err}" + (" (rate limited)" if hit_rate_limit else ""))

        # Save progress after each batch
        save_progress(done_endpoints, server_details)

        # Also update the output file after each batch
        total_with, total_res = aggregate_and_save(redm_servers, server_details)
        print(f"    Progress saved: {len(server_details)} servers, {total_res} resources")

        if hit_rate_limit:
            # For early-stopped batch, add remaining items back
            # They'll be skipped since done_endpoints was updated only for processed ones
            pass

    # Cleanup progress file
    clear_progress()

    # Final stats
    total_with, total_res = aggregate_and_save(redm_servers, server_details)
    print(f"\n{'='*50}")
    print(f"Done!")
    print(f"  RedM servers in stream: {len(redm_servers)}")
    print(f"  Servers fetched: {len(server_details)}")
    print(f"  Servers with resources: {total_with}")
    print(f"  Unique resources: {total_res}")
    print(f"  Saved to {OUTPUT_FILE}")

    # Top-20
    result = json.loads(open(OUTPUT_FILE, encoding="utf-8").read())
    top = sorted(result["resources"].items(), key=lambda x: x[1]["count"], reverse=True)[:20]
    print("\nTop-20 resources:")
    for i, (name, info) in enumerate(top, 1):
        print(f"  {i:>2}. {name} — {info['count']} servers")


# ─── Multi-worker subcommands ─────────────────────────────────────────


def cmd_prepare(workers: int):
    """Fetch stream, filter RedM servers, split into chunks for parallel workers."""
    raw = fetch_stream_with_retry()
    print(f"  Downloaded {len(raw):,} bytes")

    all_servers = read_frames(raw)
    print(f"  Total servers in stream: {len(all_servers)}")

    redm_servers = [s for s in all_servers if s.get("gamename") == "rdr3"]
    print(f"  RedM servers: {len(redm_servers)}")

    # Auto-calculate optimal workers: each gets ≤ BATCH_SIZE servers → 0 cooldowns
    if workers == 0:
        workers = max(1, math.ceil(len(redm_servers) / BATCH_SIZE))
    print(f"  Workers: {workers} (≤{BATCH_SIZE} servers each, no cooldowns needed)")

    # Split into chunks
    chunk_size = math.ceil(len(redm_servers) / workers)
    os.makedirs(CHUNKS_DIR, exist_ok=True)

    actual_chunks = 0
    for i in range(workers):
        chunk = redm_servers[i * chunk_size : (i + 1) * chunk_size]
        if not chunk:
            break
        chunk_file = os.path.join(CHUNKS_DIR, f"chunk_{i}.json")
        with open(chunk_file, "w", encoding="utf-8") as f:
            json.dump(chunk, f, ensure_ascii=False)
        print(f"  Chunk {i}: {len(chunk)} servers -> {chunk_file}")
        actual_chunks += 1

    # Save stream info for merge phase
    stream_info = {
        "total_servers": len(all_servers),
        "redm_servers": [{"endpoint": s["endpoint"]} for s in redm_servers],
        "worker_count": actual_chunks,
    }
    info_file = os.path.join(CHUNKS_DIR, "stream_info.json")
    with open(info_file, "w", encoding="utf-8") as f:
        json.dump(stream_info, f, ensure_ascii=False)

    print(f"\nDone! {actual_chunks} chunks ready in {CHUNKS_DIR}")
    print(f"  Servers per chunk: ~{chunk_size}")


def cmd_fetch(chunk_file: str, output_file: str):
    """Fetch server details for a single chunk."""
    with open(chunk_file, "r", encoding="utf-8") as f:
        servers = json.load(f)

    print(f"Fetch: processing {len(servers)} servers from {chunk_file}", flush=True)

    server_details = {}
    total_batches = math.ceil(len(servers) / BATCH_SIZE)
    print(f"  {total_batches} batches of {BATCH_SIZE} (cooldown {BATCH_COOLDOWN}s)", flush=True)

    batch_num = 0
    for batch_start in range(0, len(servers), BATCH_SIZE):
        batch = servers[batch_start : batch_start + BATCH_SIZE]
        batch_num += 1

        if batch_start > 0:
            print(f"\n  Cooldown: waiting {BATCH_COOLDOWN}s before batch {batch_num}/{total_batches}...", flush=True)
            time.sleep(BATCH_COOLDOWN)
            wait_for_unblock()

        print(f"\n  Batch {batch_num}/{total_batches}: fetching {len(batch)} servers...", flush=True)
        batch_ok = 0
        batch_err = 0
        hit_rate_limit = False

        for i, srv in enumerate(batch):
            ep = srv["endpoint"]
            detail, reason = fetch_single_server(ep)

            if reason == "rate_limit":
                hit_rate_limit = True
                batch_err += 1
                print(f"    [{i+1}/{len(batch)}] {ep} -> RATE LIMITED, stopping batch", flush=True)
                break

            if detail:
                server_details[ep] = detail
                batch_ok += 1
                status = "ok"
            else:
                batch_err += 1
                status = reason or "error"

            print(f"    [{i+1}/{len(batch)}] {ep} -> {status} (ok={batch_ok} err={batch_err})", flush=True)

        print(f"  Batch {batch_num} done: ok={batch_ok} err={batch_err}" + (" (rate limited)" if hit_rate_limit else ""), flush=True)

    # Save results
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(server_details, f, ensure_ascii=False)

    print(f"\nDone! {len(server_details)} server details saved to {output_file}")


def cmd_merge():
    """Merge all worker results and produce final resources.json."""
    # Load stream info
    info_file = os.path.join(CHUNKS_DIR, "stream_info.json")
    with open(info_file, "r", encoding="utf-8") as f:
        stream_info = json.load(f)

    redm_servers = stream_info["redm_servers"]
    print(f"Merge: {len(redm_servers)} RedM servers from stream info")

    # Load all result files
    result_files = sorted(glob_mod.glob(os.path.join(RESULTS_DIR, "result_*.json")))
    if not result_files:
        print("Error: no result files found in", RESULTS_DIR)
        return

    server_details = {}
    for rf in result_files:
        with open(rf, "r", encoding="utf-8") as f:
            chunk_details = json.load(f)
        print(f"  {rf}: {len(chunk_details)} servers")
        server_details.update(chunk_details)

    print(f"  Total: {len(server_details)} server details from {len(result_files)} workers")

    # Aggregate and save
    total_with, total_res = aggregate_and_save(redm_servers, server_details)

    print(f"\n{'='*50}")
    print(f"Done!")
    print(f"  RedM servers: {len(redm_servers)}")
    print(f"  Servers fetched: {len(server_details)}")
    print(f"  Servers with resources: {total_with}")
    print(f"  Unique resources: {total_res}")
    print(f"  Saved to {OUTPUT_FILE}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="RedM Server List Parser")
    subparsers = parser.add_subparsers(dest="command")

    # prepare
    p_prepare = subparsers.add_parser("prepare", help="Fetch stream and split into chunks")
    p_prepare.add_argument("-w", "--workers", type=int, default=0, help="Number of workers (0=auto, calculates optimal count)")

    # fetch
    p_fetch = subparsers.add_parser("fetch", help="Fetch server details for a chunk")
    p_fetch.add_argument("-c", "--chunk", required=True, help="Path to chunk JSON file")
    p_fetch.add_argument("-o", "--output", required=True, help="Path to save results JSON")

    # merge
    subparsers.add_parser("merge", help="Merge worker results into final output")

    args = parser.parse_args()

    if args.command == "prepare":
        cmd_prepare(args.workers)
    elif args.command == "fetch":
        cmd_fetch(args.chunk, args.output)
    elif args.command == "merge":
        cmd_merge()
    else:
        main()
