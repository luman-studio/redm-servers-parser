const PAGE_SIZE = 50;

document.addEventListener("alpine:init", () => {
  Alpine.data("app", () => ({
    // State
    loading: true,
    error: "",
    tab: "resources",
    query: "",
    sortField: "count",
    sortAsc: false,
    page: 0,
    hideCommon: true,
    showCommonSettings: false,
    commonSearch: "",
    commonWhitelist: [],
    commonBlacklist: [],
    detail: null,       // resource detail overlay
    serverDetail: null,  // server detail overlay

    // Data
    raw: null,
    allResources: [],
    allServers: [],
    categories: [],

    // ── Init ──
    async init() {
      this.loadCommonLists();
      this.readUrlParams();
      try {
        const resp = await fetch("data/resources.json");
        if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
        this.raw = await resp.json();
        this.allResources = Object.entries(this.raw.resources).map(([name, info]) => ({
          name, count: info.count, servers: info.servers,
        }));
        this.allServers = this.raw.servers || [];
        this.buildCategories();
        this.loading = false;
      } catch (e) {
        this.error = e.message;
        this.loading = false;
      }
    },

    // ── URL params ──
    readUrlParams() {
      const p = new URLSearchParams(location.search);
      if (p.get("q")) this.query = p.get("q");
      if (p.get("tab")) this.tab = p.get("tab");
    },

    syncUrl() {
      const p = new URLSearchParams();
      if (this.query) p.set("q", this.query);
      if (this.tab !== "resources") p.set("tab", this.tab);
      const s = p.toString();
      history.replaceState(null, "", s ? "?" + s : location.pathname);
    },

    setTab(t) { this.tab = t; this.page = 0; this.syncUrl(); },
    setQuery(q) { this.query = q; this.page = 0; this.syncUrl(); },

    _debounceTimer: null,
    debouncedSetQuery(q) {
      clearTimeout(this._debounceTimer);
      this._debounceTimer = setTimeout(() => this.setQuery(q), 250);
    },

    // ── Stats ──
    get totalServers() { return this.raw?.total_servers || 0; },
    get parsedServers() { return this.raw?.total_servers_with_resources || 0; },
    get totalResources() { return this.raw?.total_resources || 0; },
    get parsedAt() { return this.raw?.parsed_at ? new Date(this.raw.parsed_at) : null; },
    get parsedPct() { return this.totalServers > 0 ? Math.round(this.parsedServers / this.totalServers * 100) : 0; },

    get lastUpdateText() { return this.parsedAt ? relativeTime(this.parsedAt) : "—"; },

    // ── Common threshold ──
    get commonThreshold() {
      return Math.max(1, Math.floor(this.parsedServers * 0.5));
    },

    toggleHideCommon() { this.hideCommon = !this.hideCommon; this.page = 0; },

    // ── Common lists (localStorage) ──
    loadCommonLists() {
      try {
        this.commonWhitelist = JSON.parse(localStorage.getItem("redm_common_whitelist") || "[]");
        this.commonBlacklist = JSON.parse(localStorage.getItem("redm_common_blacklist") || "[]");
      } catch { this.commonWhitelist = []; this.commonBlacklist = []; }
    },

    saveCommonLists() {
      localStorage.setItem("redm_common_whitelist", JSON.stringify(this.commonWhitelist));
      localStorage.setItem("redm_common_blacklist", JSON.stringify(this.commonBlacklist));
    },

    isCommon(name) {
      if (this.commonBlacklist.includes(name)) return true;
      if (this.commonWhitelist.includes(name)) return false;
      const count = this.raw?.resources?.[name]?.count ?? 0;
      return count > this.commonThreshold;
    },

    whitelistAdd(name) {
      if (!this.commonWhitelist.includes(name)) this.commonWhitelist.push(name);
      this.commonBlacklist = this.commonBlacklist.filter(n => n !== name);
      this.saveCommonLists();
    },

    whitelistRemove(name) {
      this.commonWhitelist = this.commonWhitelist.filter(n => n !== name);
      this.saveCommonLists();
    },

    blacklistAdd(name) {
      if (!this.commonBlacklist.includes(name)) this.commonBlacklist.push(name);
      this.commonWhitelist = this.commonWhitelist.filter(n => n !== name);
      this.saveCommonLists();
    },

    blacklistRemove(name) {
      this.commonBlacklist = this.commonBlacklist.filter(n => n !== name);
      this.saveCommonLists();
    },

    // ── Common settings panel data ──
    get autoHiddenList() {
      const q = this.commonSearch.toLowerCase().trim();
      return this.allResources
        .filter(r => r.count > this.commonThreshold && !this.commonWhitelist.includes(r.name))
        .filter(r => !q || r.name.toLowerCase().includes(q))
        .sort((a, b) => b.count - a.count);
    },

    get whitelistedList() {
      const q = this.commonSearch.toLowerCase().trim();
      return this.commonWhitelist
        .map(name => ({ name, count: this.raw?.resources?.[name]?.count ?? 0 }))
        .filter(r => !q || r.name.toLowerCase().includes(q));
    },

    get blacklistedList() {
      const q = this.commonSearch.toLowerCase().trim();
      return this.commonBlacklist
        .map(name => ({ name, count: this.raw?.resources?.[name]?.count ?? 0 }))
        .filter(r => !q || r.name.toLowerCase().includes(q));
    },

    blacklistInput: "",

    addToBlacklist() {
      const name = this.blacklistInput.trim();
      if (name && this.raw?.resources?.[name]) {
        this.blacklistAdd(name);
        this.blacklistInput = "";
      }
    },

    get blacklistSuggestions() {
      const q = this.blacklistInput.toLowerCase().trim();
      if (!q || q.length < 2) return [];
      return this.allResources
        .filter(r => r.name.toLowerCase().includes(q) && !this.commonBlacklist.includes(r.name) && !this.isCommon(r.name))
        .slice(0, 8)
        .map(r => r.name);
    },

    // ── Filtered + sorted resources ──
    get filtered() {
      const q = this.query.toLowerCase().trim();
      let list = this.tab === "servers" ? [] : this.allResources;
      if (this.hideCommon && this.tab === "resources") list = list.filter(r => !this.isCommon(r.name));
      if (q && this.tab === "resources") list = list.filter(r => r.name.toLowerCase().includes(q));
      const dir = this.sortAsc ? 1 : -1;
      return [...list].sort((a, b) => {
        if (this.sortField === "count") return (a.count - b.count) * dir;
        return a.name.localeCompare(b.name) * dir;
      });
    },

    get filteredServers() {
      const q = this.query.toLowerCase().trim();
      let list = this.allServers;
      if (q) list = list.filter(s => s.name.toLowerCase().includes(q) || s.resources.some(r => r.toLowerCase().includes(q)));
      return list;
    },

    get totalPages() { return Math.max(1, Math.ceil((this.tab === "servers" ? this.filteredServers : this.filtered).length / PAGE_SIZE)); },
    get pageItems() {
      const src = this.tab === "servers" ? this.filteredServers : this.filtered;
      return src.slice(this.page * PAGE_SIZE, (this.page + 1) * PAGE_SIZE);
    },

    toggleSort(field) {
      if (this.sortField === field) { this.sortAsc = !this.sortAsc; }
      else { this.sortField = field; this.sortAsc = field === "name"; }
    },

    sortArrow(field) { return this.sortField === field ? (this.sortAsc ? " \u25B2" : " \u25BC") : ""; },
    isSorted(field) { return this.sortField === field; },

    prevPage() { if (this.page > 0) this.page--; },
    nextPage() { if (this.page < this.totalPages - 1) this.page++; },

    // ── Resource detail ──
    openResource(name) {
      const r = this.allResources.find(x => x.name === name);
      if (!r) return;
      this.detail = { ...r, correlated: this.getCorrelated(name) };
    },

    closeDetail() { this.detail = null; },

    getCorrelated(name) {
      const r = this.allResources.find(x => x.name === name);
      if (!r) return [];
      const serverIds = new Set(r.servers.map(s => s.id));
      const counts = {};
      for (const srv of this.allServers) {
        if (!serverIds.has(srv.id)) continue;
        for (const res of srv.resources) {
          if (res === name) continue;
          counts[res] = (counts[res] || 0) + 1;
        }
      }
      return Object.entries(counts)
        .filter(([n]) => !this.hideCommon || !this.isCommon(n))
        .map(([n, c]) => ({ name: n, count: c, pct: Math.round(c / r.count * 100) }))
        .sort((a, b) => b.count - a.count)
        .slice(0, 30);
    },

    // ── Server detail ──
    openServer(srv) {
      this.serverDetail = srv;
    },

    closeServerDetail() { this.serverDetail = null; },

    // ── Categories ──
    buildCategories() {
      const prefixCounts = {};
      for (const r of this.allResources) {
        const m = r.name.match(/^([a-zA-Z]{2,})[_-]/);
        if (!m) continue;
        const prefix = m[0].toLowerCase();
        if (!prefixCounts[prefix]) prefixCounts[prefix] = { resources: 0 };
        prefixCounts[prefix].resources++;
      }
      this.categories = Object.entries(prefixCounts)
        .filter(([, v]) => v.resources >= 3)
        .map(([prefix, v]) => ({ prefix, resources: v.resources }))
        .sort((a, b) => b.resources - a.resources)
        .slice(0, 40);
    },

    filterByCategory(prefix) {
      this.tab = "resources";
      this.query = prefix;
      this.page = 0;
      this.syncUrl();
    },

    // ── Charts data ──
    get nonCommonResources() {
      return this.hideCommon ? this.allResources.filter(r => !this.isCommon(r.name)) : this.allResources;
    },

    get topResources() {
      return [...this.nonCommonResources].sort((a, b) => b.count - a.count).slice(0, 15);
    },

    get maxCount() {
      return this.topResources.length ? this.topResources[0].count : 1;
    },

    get histogram() {
      const buckets = [
        { label: "1", min: 1, max: 1 },
        { label: "2-3", min: 2, max: 3 },
        { label: "4-10", min: 4, max: 10 },
        { label: "11-25", min: 11, max: 25 },
        { label: "26-50", min: 26, max: 50 },
        { label: "51+", min: 51, max: Infinity },
      ];
      const counts = buckets.map(() => 0);
      for (const r of this.nonCommonResources) {
        for (let i = 0; i < buckets.length; i++) {
          if (r.count >= buckets[i].min && r.count <= buckets[i].max) { counts[i]++; break; }
        }
      }
      const max = Math.max(...counts, 1);
      return buckets.map((b, i) => ({ label: b.label, count: counts[i], pct: Math.round(counts[i] / max * 100) }));
    },

    // ── CSV export ──
    exportCsv() {
      const src = this.tab === "servers" ? this.filteredServers : this.filtered;
      let csv;
      if (this.tab === "servers") {
        csv = "Server,Players,Max Players,Resources Count,Endpoint\n" +
          src.map(s => `"${s.name.replace(/"/g, '""')}",${s.players},${s.max_players},${s.resources.length},${s.id}`).join("\n");
      } else {
        csv = "Resource,Server Count\n" +
          src.map(r => `"${r.name.replace(/"/g, '""')}",${r.count}`).join("\n");
      }
      const blob = new Blob([csv], { type: "text/csv" });
      const a = document.createElement("a");
      a.href = URL.createObjectURL(blob);
      a.download = `redm_${this.tab}_${new Date().toISOString().slice(0, 10)}.csv`;
      a.click();
    },

    // ── Helpers ──
    esc(s) { const d = document.createElement("div"); d.textContent = s; return d.innerHTML; },
    fmt(n) { return n.toLocaleString(); },
    serverLink(id) { return `https://servers.redm.net/servers/detail/${id}`; },
  }));
});

// ── Utility functions ──

function relativeTime(date, future = false) {
  const now = new Date();
  const diff = future ? date - now : now - date;
  const sec = Math.floor(diff / 1000);
  if (sec < 60) return future ? "< 1 min" : "just now";
  const min = Math.floor(sec / 60);
  const suffix = future ? "from now" : "ago";
  if (min < 60) return `${min}m ${suffix}`;
  const hrs = Math.floor(min / 60);
  if (hrs < 24) return `${hrs}h ${min % 60}m ${suffix}`;
  const days = Math.floor(hrs / 24);
  return `${days}d ${hrs % 24}h ${suffix}`;
}
