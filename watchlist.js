// The watch list: bookmark movies, tick them off once watched, put them in order, and filter the results by them.
//
// Self-contained, like analytics.js. index.html loads this file with one <script> tag and calls into it from a few
// one-line hooks marked "watch list hook", each written so the page works the same without this file. To remove the
// feature, delete this file, manifest.webmanifest, the script tag and those lines.
//
// What it keeps on the device (localStorage), and nothing about the movies themselves:
//   watchlist       {"v":1,"items":[[771,0],[10719,1]]}  TMDB ids in the user's order, with 1 for watched
//   watchlistPrefs  {"v":1,...}                          the two filter pills, Hide watched, and the Home Screen tip
// Titles, posters and the rest come from the page's own data (db.movies). Ids the data doesn't have are kept but
// not shown, so a movie that drops out of the data comes back if it returns.
(() => {
    // ---------- Storage ----------
    const LIST_KEY = 'watchlist';
    const PREFS_KEY = 'watchlistPrefs';
    const MAX_MOVIES = 2000; // far beyond any real list; a guard against garbage

    function readJSON(key) {
        try { return JSON.parse(localStorage.getItem(key)); } catch (e) { return null; }
    }

    function writeJSON(key, value) {
        try { localStorage.setItem(key, JSON.stringify(value)); } catch (e) {}
    }

    // A TMDB id as the page's data spells it: digits without leading zeros, as a string. Anything else is null.
    function cleanId(raw) {
        const text = String(raw ?? '').trim();
        return /^\d{1,9}$/.test(text) ? String(Number(text)) : null;
    }

    // Valid entries only, each movie once, in order
    function cleanItems(list) {
        const seen = new Set();
        const clean = [];
        for (const entry of list) {
            const id = cleanId(entry?.id);
            if (!id || seen.has(id)) continue;
            seen.add(id);
            clean.push({ id, watched: !!entry.watched });
            if (clean.length >= MAX_MOVIES) break;
        }
        return clean;
    }

    function loadItems() {
        const saved = readJSON(LIST_KEY);
        if (saved?.v !== 1 || !Array.isArray(saved.items)) return [];
        return cleanItems(saved.items.map(entry => (Array.isArray(entry) ? { id: entry[0], watched: entry[1] } : null)));
    }

    function saveItems() {
        writeJSON(LIST_KEY, { v: 1, items: items.map(item => [Number(item.id), item.watched ? 1 : 0]) });
    }

    function loadPrefs() {
        const saved = readJSON(PREFS_KEY) || {};
        const triState = value => ([0, 1, 2].includes(value) ? value : 0);
        return {
            listed: triState(saved.listed),     // the On My List pill: 0 off, 1 only these, 2 hide these
            watched: triState(saved.watched),   // the Watched pill, the same way
            hideWatched: saved.hideWatched === true,
            tipShown: saved.tipShown === true,  // the Home Screen tip has popped up after the first add
            tipDone: saved.tipDone === true,    // ...and was closed, so it never shows again
        };
    }

    const savePrefs = () => writeJSON(PREFS_KEY, { v: 1, ...prefs });

    let items = loadItems(); // [{ id: '771', watched: false }, ...] in the user's order
    const prefs = loadPrefs();

    // Ask the browser not to clear the site's storage when space runs low (a no-op where unsupported)
    function persistStorage() {
        try { navigator.storage?.persist?.()?.catch?.(() => {}); } catch (e) {}
    }

    // ---------- The page's movie data ----------
    // db is index.html's movie data; until it loads, every id counts as unknown
    const movieFor = id => (typeof db === 'object' && db?.movies?.[id]) || null;
    const shownItems = () => items.filter(item => movieFor(item.id));
    const itemFor = id => items.find(item => item.id === id);
    const plural = (n, word) => `${n} ${word}${n === 1 ? '' : 's'}`;

    // ---------- Changing the list ----------
    function add(id) {
        id = cleanId(id);
        if (!id || itemFor(id)) return;
        items.push({ id, watched: false });
        if (items.length === 1) persistStorage();
        changed();
    }

    function remove(id) {
        const index = items.findIndex(item => item.id === id);
        if (index < 0) return;
        items.splice(index, 1);
        changed();
    }

    function setWatched(id, watched) {
        const item = itemFor(id);
        if (!item || item.watched === !!watched) return;
        item.watched = !!watched;
        changed();
    }

    const toggle = id => (itemFor(id) ? remove(id) : add(id));

    // A new order for the movies on screen. The ones not on screen (hidden as watched, narrowed by search, or
    // missing from the data) keep their places.
    function reorder(visibleIds) {
        const visible = new Set(visibleIds);
        const slots = [];
        items.forEach((item, index) => { if (visible.has(item.id)) slots.push(index); });
        if (slots.length !== visibleIds.length || visible.size !== visibleIds.length) return;
        const next = items.slice();
        visibleIds.forEach((id, k) => { next[slots[k]] = itemFor(id); });
        items = next;
        changed();
    }

    // After every change: save, then bring the results, buttons, filters and the open list up to date
    function changed({ save = true } = {}) {
        if (save) saveItems();
        const refilter = filtersOn(); // the results depend on the list right now
        if (!shownItems().length) resetFilters(); // an empty list has nothing to filter by
        refreshChrome();
        if (refilter) window.updateResults?.();
    }

    // Another tab changed the list
    addEventListener('storage', e => {
        if (e.key !== LIST_KEY || !started) return;
        items = loadItems();
        changed({ save: false });
    });

    // ---------- Hooks for index.html ----------
    // The two pills in the filters' My Watchlist section. Like every other pill: tap once for only these, again
    // to hide them, a third time for all. Remembered on the device, and never part of a shared link.
    const FILTERS = [
        { key: 'listed', label: 'On My List', hint: 'Movies on My Watchlist' },
        { key: 'watched', label: 'Watched', hint: "Movies you've marked watched" },
    ];
    const filtersOn = () => !!(prefs.listed || prefs.watched);
    // 0 lets every movie through, 1 only the matches, 2 only the rest
    const passes = (state, matches) => state === 0 || (state === 1) === matches;

    // updateResults: narrow the movie ids by the two pills
    function filter(ids) {
        start();
        if (!filtersOn()) return ids;
        const byId = new Map(items.map(item => [item.id, item]));
        return ids.filter(id => {
            const item = byId.get(id);
            return passes(prefs.listed, !!item) && passes(prefs.watched, !!item?.watched);
        });
    }

    // renderPills: a chip above the results for each pill that's on, using the page's own addPill
    function addPills(addPill) {
        FILTERS.forEach(({ key, label }) => {
            const state = prefs[key];
            if (state) addPill(state === 2 ? `NO: ${label}` : label, state === 2, () => setFilter(key, 0));
        });
    }

    // hasActiveFilters: so Clear filters shows when only these are on
    const hasActiveFilters = () => filtersOn();

    // clearTagsAndSearch (Clear filters, Reset filters): index.html redraws the results right after
    const clearFilters = () => resetFilters();

    function resetFilters() {
        if (!filtersOn()) return;
        prefs.listed = prefs.watched = 0;
        savePrefs();
        syncFilterPills();
    }

    function setFilter(key, state) {
        prefs[key] = state;
        savePrefs();
        syncFilterPills();
        window.updateResults?.();
    }

    // ---------- The My Watchlist section in the filters, under Holiday Intensity ----------
    // The same look as the other filter pills: this is updateBtnState's class string in index.html
    const PILL_CLASS = 'px-3 py-1 rounded-full border text-[11px] font-semibold transition-all whitespace-nowrap';
    const STATE_CLASSES = ['state-neutral', 'state-include', 'state-exclude'];
    const STATE_WORDS = ['all movies', 'only these', 'hidden'];

    function buildFilterSection() {
        const levels = document.getElementById('level-section');
        if (!levels) return;
        const section = document.createElement('div');
        section.id = 'wl-filters';
        section.className = 'hidden';
        section.innerHTML = '<h3 class="text-[10px] font-bold text-slate-400 uppercase tracking-widest mb-3">My Watchlist</h3><div class="flex flex-wrap gap-2"></div>';
        FILTERS.forEach(({ key, label, hint }) => {
            const btn = document.createElement('button');
            btn.type = 'button';
            btn.id = `wl-filter-${key}`;
            btn.textContent = label;
            btn.title = hint;
            btn.onclick = () => setFilter(key, (prefs[key] + 1) % 3);
            section.lastElementChild.appendChild(btn);
        });
        levels.after(section);
        syncFilterPills();
    }

    function syncFilterPills() {
        FILTERS.forEach(({ key, label }) => {
            const btn = document.getElementById(`wl-filter-${key}`);
            if (!btn) return;
            btn.className = `${PILL_CLASS} ${STATE_CLASSES[prefs[key]]}`;
            btn.setAttribute('aria-label', `${label}: ${STATE_WORDS[prefs[key]]}`);
        });
    }

    // Everything that shows whether the list has movies in it
    function refreshChrome() {
        const count = shownItems().length;
        document.body.classList.toggle('wl-has-list', count > 0);
        document.getElementById('wl-filters')?.classList.toggle('hidden', count === 0);
    }

    // ---------- Styles ----------
    const CSS = `
        /* The filters' My Watchlist section: room for the fold button when it's the first heading */
        @media (max-width: 1023px) {
            #level-section.hidden + #wl-filters h3, #level-section.hidden + #wl-filters.hidden + div h3 { padding-right: 30px; }
        }
    `;
    const style = document.createElement('style');
    style.textContent = CSS;
    document.head.appendChild(style);

    // ---------- Start ----------
    // On the first updateResults, once the page has its data
    let started = false;
    function start() {
        if (started) return;
        started = true;
        if (!shownItems().length) resetFilters();
        buildFilterSection();
        refreshChrome();
    }

    window.watchlist = {
        // Hooks for index.html
        filter, addPills, hasActiveFilters, clearFilters,
        // For the console and tests
        add, remove, toggle, setWatched, reorder,
        items: () => items.map(item => ({ ...item })),
    };
})();
