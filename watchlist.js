// The watch list: bookmark movies, tick them off once watched, put them in order, and filter the results by them.
//
// Self-contained, like analytics.js. index.html loads this file with one <script> tag and calls into it from a few
// one-line hooks marked "watch list hook", each written so the page works the same without this file. To remove the
// feature, delete this file, manifest.webmanifest, the script tag and those lines.
//
// What it keeps on the device (localStorage), and nothing about the movies themselves:
//   watchlist       {"v":1,"items":[[771,0],[10719,1]],"made":"2026-11-09T19:41Z","clearedAt":"2026-11-10T02:15:09.411Z"}
//                   TMDB ids in the user's order with 1 for watched, when the list last changed, and when Clear
//                   list last emptied it here (so an older copy in the address doesn't bring it back)
//   watchlistPrefs  {"v":1,...}  the two filter pills, Hide watched, and the Home Screen tip
// Titles, posters and the rest come from the page's own data (db.movies). Ids the data doesn't have are kept but
// not shown, so a movie that drops out of the data comes back if it returns.
//
// It also keeps a copy of the list in the address (see "The list in the address" below), because storage doesn't
// always follow the user: an iPhone Home Screen app starts with its own, empty storage, and so does a private
// window. Opening that address on an empty list brings the list back.
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

    // A list saved by a later version of this file (v 2 and up) can't be read here. This version then leaves it
    // alone: it shows an empty list and saves nothing over it, so an old tab left open can't erase it.
    const laterVersion = saved => typeof saved?.v === 'number' && saved.v > 1;

    function loadItems() {
        const saved = readJSON(LIST_KEY);
        if (saved?.v !== 1 || !Array.isArray(saved.items)) return [];
        return cleanItems(saved.items.map(entry => (Array.isArray(entry) ? { id: entry[0], watched: entry[1] } : null)));
    }

    // When the list last changed (the made= of the copy in the address), or null
    const loadMade = () => parseMade(readJSON(LIST_KEY)?.made);
    const minuteISO = date => `${date.toISOString().slice(0, 16)}Z`; // 2026-11-09T19:41Z

    // When Clear list last emptied the list on this device, in milliseconds, or null
    function loadClearedAt() {
        const time = Date.parse(readJSON(LIST_KEY)?.clearedAt);
        return Number.isFinite(time) ? time : null;
    }

    // Fields this version doesn't know about (added by a later one) are kept
    function saveItems() {
        const saved = readJSON(LIST_KEY);
        if (laterVersion(saved)) return;
        writeJSON(LIST_KEY, {
            ...(saved && typeof saved === 'object' && !Array.isArray(saved) ? saved : {}),
            v: 1,
            items: items.map(item => [Number(item.id), item.watched ? 1 : 0]),
            made: listMade ? minuteISO(listMade) : undefined,
            clearedAt: clearedAt === null ? undefined : new Date(clearedAt).toISOString(),
        });
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

    function savePrefs() {
        const saved = readJSON(PREFS_KEY);
        if (laterVersion(saved)) return;
        writeJSON(PREFS_KEY, { ...(saved && typeof saved === 'object' && !Array.isArray(saved) ? saved : {}), v: 1, ...prefs });
    }

    let items = loadItems(); // [{ id: '771', watched: false }, ...] in the user's order
    let listMade = loadMade();
    let clearedAt = loadClearedAt();
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
    const yearOf = m => (m.date && m.date !== '0000-00-00' ? m.date.split('-')[0] : 'TBA');
    const ratingOf = m => (m.votes ? `${m.rating.toFixed(1)}/10` : 'Not yet rated');
    const tmdbURL = id => `https://www.themoviedb.org/movie/${id}`;
    // The poster at a TMDB size that fits its thumbnail (the data has w500): w185 for list rows, w342 for the
    // peek, w154 for the card's strip. A URL without TMDB's usual /t/p/w500/ stays as it is.
    const posterAt = (url, size) => (url || '').replace('/t/p/w500/', `/t/p/${size}/`);
    // Movie text going into HTML: titles have quotes, apostrophes and ampersands
    const ESCAPES = { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' };
    const esc = text => String(text ?? '').replace(/[&<>"']/g, c => ESCAPES[c]);

    // ---------- Icons (lucide's, inline so each mark is cheap to draw) ----------
    const BOOKMARK = '<path d="M17 3a2 2 0 0 1 2 2v15a1 1 0 0 1-1.496.868l-4.512-2.578a2 2 0 0 0-1.984 0l-4.512 2.578A1 1 0 0 1 5 20V5a2 2 0 0 1 2-2z"/>';
    const ICONS = {
        bookmark: BOOKMARK,
        bookmarkPlus: `${BOOKMARK}<path d="M12 7v6"/><path d="M15 10H9"/>`,
        bookmarkMinus: `${BOOKMARK}<path d="M15 10H9"/>`,
        check: '<path d="M20 6 9 17l-5-5"/>',
        x: '<path d="M18 6 6 18"/><path d="m6 6 12 12"/>',
        eye: '<path d="M2.062 12.348a1 1 0 0 1 0-.696 10.75 10.75 0 0 1 19.876 0 1 1 0 0 1 0 .696 10.75 10.75 0 0 1-19.876 0"/><circle cx="12" cy="12" r="3"/>',
        eyeOff: '<path d="M10.733 5.076a10.744 10.744 0 0 1 11.205 6.575 1 1 0 0 1 0 .696 10.747 10.747 0 0 1-1.444 2.49"/><path d="M14.084 14.158a3 3 0 0 1-4.242-4.242"/><path d="M17.479 17.499a10.75 10.75 0 0 1-15.417-5.151 1 1 0 0 1 0-.696 10.75 10.75 0 0 1 4.446-5.143"/><path d="m2 2 20 20"/>',
        externalLink: '<path d="M15 3h6v6"/><path d="M10 14 21 3"/><path d="M18 13v6a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2V8a2 2 0 0 1 2-2h6"/>',
        chevronLeft: '<path d="m15 18-6-6 6-6"/>',
        trash: '<path d="M3 6h18"/><path d="M19 6v14c0 1-1 2-2 2H7c-1 0-2-1-2-2V6"/><path d="M8 6V4c0-1 1-2 2-2h4c1 0 2 1 2 2v2"/>',
        clock: '<circle cx="12" cy="12" r="10"/><path d="M12 6v6l4 2"/>',
        // Safari's Share icon, which the Home Screen tip points to (not the page's own Share results icon)
        share: '<path d="M12 2v13"/><path d="m16 6-4-4-4 4"/><path d="M4 12v8a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2v-8"/>',
        grip: '<circle cx="9" cy="5" r="1.6"/><circle cx="9" cy="12" r="1.6"/><circle cx="9" cy="19" r="1.6"/><circle cx="15" cy="5" r="1.6"/><circle cx="15" cy="12" r="1.6"/><circle cx="15" cy="19" r="1.6"/>',
    };

    function icon(name, size, { fill = 'none', stroke = 2, cls = '', label = '' } = {}) {
        const a11y = label ? `role="img" aria-label="${label}"` : 'aria-hidden="true"';
        return `<svg ${a11y} class="${cls}" width="${size}" height="${size}" viewBox="0 0 24 24" fill="${fill}" stroke="currentColor" stroke-width="${stroke}" stroke-linecap="round" stroke-linejoin="round">${ICONS[name]}</svg>`;
    }

    // ---------- Changing the list ----------
    function add(id) {
        id = cleanId(id);
        if (!id || itemFor(id) || items.length >= MAX_MOVIES) return;
        items.push({ id, watched: false });
        if (items.length === 1) persistStorage();
        changed();
        firstAddTip(movieFor(id)?.title ?? 'it');
    }

    function remove(id) {
        id = cleanId(id);
        const index = items.findIndex(item => item.id === id);
        if (index < 0) return;
        const [item] = items.splice(index, 1);
        changed();
        offerUndo(`Removed ${movieFor(id)?.title ?? 'a movie'}`, () => {
            if (itemFor(item.id)) return;
            items.splice(Math.min(index, items.length), 0, item); // back in its place, watched or not
            changed();
        });
    }

    function setWatched(id, watched) {
        const item = itemFor(cleanId(id));
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

    // After every change: save, then bring the results, buttons, filters, the open list and the address up to date.
    // `made` is when the list changed: now, or a restored copy's own time.
    function changed({ save = true, made = new Date() } = {}) {
        if (save) {
            listMade = made;
            saveItems();
        }
        const refilter = filtersOn(); // the results depend on the list right now
        if (!shownItems().length) resetFilters(); // an empty list has nothing to filter by
        refreshChrome();
        if (refilter) window.updateResults?.({ keepOrder: true }); // only the list changed: Random keeps its order
        else markCards();
        renderView();
        syncAddress();
    }

    function reloadList() {
        items = loadItems();
        listMade = loadMade();
        clearedAt = loadClearedAt();
    }

    // Another tab changed the list
    addEventListener('storage', e => {
        if (e.key !== LIST_KEY || !started) return;
        reloadList();
        changed({ save: false });
    });

    // Back to a page kept in the back/forward cache: another tab may have changed the list meanwhile
    addEventListener('pageshow', e => {
        if (!e.persisted || !started) return;
        reloadList();
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
        try { start(); } catch (e) { console.error(e); }
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

    // ---------- Marks on the result cards ----------
    // A movie on the list gets a blue bookmark at the poster's top right; a watched one is faded, with a green
    // Watched ribbon across the top left corner. index.html redraws the cards on every change, so each new set
    // is marked as it lands, without touching renderGrid.
    function watchGrid() {
        const grid = document.getElementById('results-grid');
        if (!grid) return;
        new MutationObserver(cardsRedrawn).observe(grid, { childList: true });
        grid.addEventListener('click', onCardClick);
    }

    function cardsRedrawn() {
        markCards();
        if (pageMode()) renderView(); // the search may have changed, and on the page it narrows the list
    }

    function markCards() {
        const byId = new Map(items.map(item => [item.id, item]));
        document.querySelectorAll('#results-grid a.movie-card').forEach(card => {
            if (!('wlId' in card.dataset)) setUpCard(card);
            const id = card.dataset.wlId;
            if (!id) return;
            const item = byId.get(id);
            card.classList.toggle('wl-on', !!item);
            card.classList.toggle('wl-watched', !!item?.watched);
            const mark = card.querySelector('.wl-mark');
            if (mark.dataset.on === String(!!item)) return;
            mark.dataset.on = !!item;
            const title = movieFor(id)?.title || 'this movie';
            mark.setAttribute('aria-label', item ? `Remove ${title} from My Watchlist` : `Add ${title} to My Watchlist`);
            mark.lastElementChild.textContent = item ? 'Remove from My Watchlist' : 'Add to My Watchlist';
        });
    }

    // The ribbon and the bookmark go in the poster box, beside its image
    function setUpCard(card) {
        const id = cleanId(card.getAttribute('href')?.match(/\/movie\/(\d+)/)?.[1]);
        const poster = card.querySelector('img')?.parentElement;
        card.dataset.wlId = id && poster ? id : '';
        if (!card.dataset.wlId) return;
        poster.classList.add('wl-poster');
        poster.insertAdjacentHTML('beforeend', '<span class="wl-ribbon">Watched</span>'
            + `<button type="button" class="wl-mark">${icon('bookmark', 24, { fill: 'currentColor', cls: 'wl-mark-on' })}`
            + `${icon('bookmarkPlus', 24, { stroke: 2.25, cls: 'wl-mark-off' })}<span class="wl-mark-label" aria-hidden="true"></span></button>`);
    }

    // With a mouse, the bookmark adds or removes, and the rest of the card opens TMDB as before.
    // On a touch screen, the poster opens the peek instead of TMDB.
    let lastPointerType = '';
    addEventListener('pointerdown', e => { lastPointerType = e.pointerType; }, true);
    const mouseDevice = matchMedia('(hover: hover) and (pointer: fine)');
    // A finger or pen, not a mouse; keys (Enter, Space) click with detail 0 and count as a mouse
    const isTap = e => e.detail !== 0 && (lastPointerType ? lastPointerType !== 'mouse' : !mouseDevice.matches);

    function onCardClick(e) {
        const card = e.target.closest('a.movie-card');
        const id = card?.dataset.wlId;
        if (!id) return;
        if (isTap(e) && e.target.closest('.wl-poster')) {
            e.preventDefault();
            openPeek(id);
        } else if (e.target.closest('.wl-mark')) {
            e.preventDefault();
            const adding = !itemFor(id);
            toggle(id);
            if (adding) pop(id);
        }
    }

    // A little bounce for the bookmark of a movie just added
    function pop(id) {
        queueMicrotask(() => { // after the cards are redrawn, if they were
            const mark = document.querySelector(`#results-grid a[data-wl-id="${id}"] .wl-mark`);
            if (mark) replay(mark, 'wl-pop');
        });
    }

    // Restart a CSS animation
    function replay(el, cls) {
        el.classList.remove(cls);
        void el.offsetWidth;
        el.classList.add(cls);
    }

    // ---------- Layers: sheets and cards over the page ----------
    // Each has a dimmed backdrop and a panel. While one is open, the page behind can't be scrolled, tabbed to or
    // read out; Escape or a tap on the backdrop dismisses the top one, and focus goes back where it was.
    const openLayers = [];

    function createLayer(id, panelClass, dismiss) {
        const el = document.createElement('div');
        el.id = id;
        el.className = 'wl-layer';
        el.hidden = true;
        el.innerHTML = `<div class="wl-backdrop"></div><section class="${panelClass}" role="dialog" aria-modal="true" tabindex="-1"></section>`;
        el.firstElementChild.addEventListener('click', () => layer.dismiss());
        document.body.appendChild(el);
        const layer = { el, panel: el.lastElementChild, dismiss, timer: 0, returnTo: null };
        return layer;
    }

    function openLayer(layer, focusTarget) {
        clearTimeout(layer.timer);
        if (!openLayers.includes(layer)) {
            layer.returnTo = document.activeElement;
            openLayers.push(layer);
        }
        layer.el.hidden = false;
        updateModal();
        void layer.el.offsetWidth; // so it slides in
        layer.el.classList.add('wl-shown');
        (focusTarget || layer.panel).focus({ preventScroll: true });
    }

    function closeLayer(layer) {
        const index = openLayers.indexOf(layer);
        if (index < 0) return;
        openLayers.splice(index, 1);
        layer.el.classList.remove('wl-shown');
        updateModal();
        layer.timer = setTimeout(() => { layer.el.hidden = true; }, 300);
        if (layer.returnTo?.isConnected) layer.returnTo.focus({ preventScroll: true });
        layer.returnTo = null;
    }

    function updateModal() {
        const top = openLayers[openLayers.length - 1];
        document.documentElement.classList.toggle('wl-locked', !!top);
        document.body.classList.toggle('wl-modal-open', !!top);
        for (const el of document.body.children) {
            if (el.tagName !== 'SCRIPT' && el.id !== 'wl-toasts' && el.id !== 'wl-live') el.inert = !!top && el !== top.el;
        }
    }

    // Escape closes the top layer, or the My Watchlist page (unless typing in the search)
    addEventListener('keydown', e => {
        if (e.key !== 'Escape' || !started) return;
        if (openLayers.length) openLayers[openLayers.length - 1].dismiss();
        else if (viewOpen && !e.target.closest?.('input, select, textarea')) closeView();
        else return;
        e.preventDefault();
    });

    // A drag that ends a gesture shouldn't also count as a tap on whatever it ended over
    let suppressClickUntil = 0;
    const suppressClick = () => { suppressClickUntil = performance.now() + 400; };
    addEventListener('click', e => {
        if (performance.now() > suppressClickUntil) return;
        suppressClickUntil = 0;
        e.preventDefault();
        e.stopPropagation();
    }, true);

    // Swipe a sheet down to close it; a short drag springs back. Not from its buttons and links.
    function dragToClose(handle, sheet, close, enabled = () => true) {
        handle.addEventListener('pointerdown', e => {
            if (!enabled() || e.button > 0 || e.target.closest('button, a')) return;
            const startY = e.clientY;
            let dy = 0;
            let dragging = false;
            let recent = [{ dy: 0, t: e.timeStamp }]; // for the speed at the end: a quick flick closes it too
            const move = ev => {
                if (ev.pointerId !== e.pointerId) return;
                dy = Math.max(0, ev.clientY - startY);
                recent = [...recent.filter(s => ev.timeStamp - s.t < 100), { dy, t: ev.timeStamp }];
                if (!dragging && dy < 6) return;
                dragging = true;
                sheet.style.transition = 'none';
                sheet.style.transform = `translateY(${dy}px)`;
            };
            const end = ev => {
                if (ev.pointerId !== e.pointerId) return;
                removeEventListener('pointermove', move);
                removeEventListener('pointerup', end);
                removeEventListener('pointercancel', end);
                if (!dragging) return;
                suppressClick();
                sheet.style.transition = '';
                sheet.style.transform = ''; // springs back, or slides on down as it closes
                const [first] = recent;
                const fast = (dy - first.dy) / Math.max(16, ev.timeStamp - first.t) > 0.6;
                if (ev.type === 'pointerup' && (dy > 90 || (fast && dy > 30))) close();
            };
            addEventListener('pointermove', move);
            addEventListener('pointerup', end);
            addEventListener('pointercancel', end);
        });
    }

    // ---------- The peek: a tap on a poster, on touch screens ----------
    let peek;
    let peekId = null;

    function buildPeek() {
        peek = createLayer('wl-peek', 'wl-peek-sheet', closePeek);
        peek.panel.setAttribute('aria-labelledby', 'wl-peek-title');
        peek.panel.addEventListener('click', onPeekClick);
        dragToClose(peek.panel, peek.panel, closePeek);
    }

    function openPeek(id) {
        if (!movieFor(id)) return;
        peekId = id;
        renderPeek();
        openLayer(peek, peek.panel.querySelector('h3'));
    }

    const closePeek = () => closeLayer(peek);

    function renderPeek() {
        const m = movieFor(peekId);
        const item = itemFor(peekId);
        const status = item
            ? `<div class="wl-peek-status">${icon('bookmark', 11, { fill: 'currentColor' })}On My Watchlist${item.watched ? ' · Watched' : ''}</div>`
            : '';
        const actions = !item
            ? `<button type="button" class="wl-btn wl-btn-blue wl-peek-add" data-peek="add">${icon('bookmarkPlus', 18, { stroke: 2.25 })}Add to My Watchlist</button>`
            : `<div class="wl-peek-pair">${item.watched
                ? `<button type="button" class="wl-btn" data-peek="unwatched">${icon('eyeOff', 17)}Not watched</button>`
                : `<button type="button" class="wl-btn wl-btn-green" data-peek="watched">${icon('eye', 17)}Watched it</button>`}`
              + `<button type="button" class="wl-btn" data-peek="remove">${icon('bookmarkMinus', 17, { stroke: 2.25 })}Remove</button></div>`;
        peek.panel.innerHTML = `<div class="wl-grabber" aria-hidden="true"></div>
            <div class="wl-peek-top">
                ${m.poster ? `<img src="${esc(posterAt(m.poster, 'w342'))}" alt="" decoding="async" draggable="false">` : '<span class="wl-noposter"></span>'}
                <div class="wl-peek-text">
                    ${status}
                    <h3 id="wl-peek-title" tabindex="-1">${esc(m.title)} <span class="wl-year">(${yearOf(m)})</span></h3>
                    <div class="wl-meta"><span class="wl-cert">${esc(m.cert)}</span><span class="wl-rating">${ratingOf(m)}</span></div>
                    <p class="wl-peek-overview">${esc(m.overview)}</p>
                </div>
            </div>
            ${actions}
            <a class="wl-btn wl-btn-tmdb" href="${tmdbURL(peekId)}" target="_blank" rel="noopener" draggable="false" data-peek="tmdb">More on TMDB ${icon('externalLink', 14)}</a>`;
    }

    // Each choice closes the peek, and the poster behind shows what changed
    function onPeekClick(e) {
        const action = e.target.closest('[data-peek]')?.dataset.peek;
        if (!action) return;
        const id = peekId;
        closePeek(); // and 'tmdb', a link, goes on to open TMDB in a new tab
        if (action === 'add') {
            add(id);
            setTimeout(() => pop(id), 250); // once the peek is out of the way
        } else if (action === 'watched' || action === 'unwatched') {
            setWatched(id, action === 'watched');
        } else if (action === 'remove') {
            remove(id);
        }
    }

    // ---------- The My List buttons: floating on phones and tablets, in the header row on laptops ----------
    let fab;
    let headerButton;
    let shownCount = 0;

    function buildButtons() {
        fab = document.createElement('button');
        fab.type = 'button';
        fab.id = 'wl-fab';
        fab.innerHTML = `${icon('bookmark', 17, { fill: 'currentColor' })}<span class="wl-fab-label">My List</span><span class="wl-count"></span>`;
        fab.onclick = showMyWatchlist;
        document.body.appendChild(fab);

        headerButton = document.createElement('button');
        headerButton.type = 'button';
        headerButton.id = 'wl-header-button';
        headerButton.title = 'My Watchlist';
        headerButton.innerHTML = `${icon('bookmark', 16, { fill: 'currentColor' })}<span class="wl-fab-label">My Watchlist</span><span class="wl-count"></span>`;
        headerButton.onclick = () => (viewOpen && !looking ? closeView() : showMyWatchlist());
        document.getElementById('sort-dir')?.parentElement?.after(headerButton); // beside search and sort
    }

    // Everything that shows whether the list has movies in it, and how many
    function refreshChrome() {
        const count = shownItems().length;
        document.body.classList.toggle('wl-has-list', count > 0);
        document.getElementById('wl-filters')?.classList.toggle('hidden', count === 0);
        [[fab, 'My List'], [headerButton, 'My Watchlist']].forEach(([btn, name]) => {
            if (!btn) return;
            const counter = btn.querySelector('.wl-count');
            counter.textContent = count;
            if (count !== shownCount) replay(counter, 'wl-bump'); // a little bounce when it changes
            btn.setAttribute('aria-label', `${name}, ${plural(count, 'movie')}`);
        });
        shownCount = count;
    }

    // ---------- Toasts: Undo and short notes, at the bottom of the screen ----------
    let toastBox;
    let liveRegion;

    function buildToasts() {
        toastBox = document.createElement('div');
        toastBox.id = 'wl-toasts';
        liveRegion = document.createElement('div');
        liveRegion.id = 'wl-live';
        liveRegion.className = 'wl-sr';
        liveRegion.setAttribute('aria-live', 'polite');
        document.body.append(toastBox, liveRegion);
    }

    // Tell screen readers about something that changed out of their sight
    function announce(text) {
        liveRegion.textContent = '';
        setTimeout(() => { liveRegion.textContent = text; }, 50);
    }

    // A toast stays up for `timeout` ms, longer while it's pointed at or focused
    function toast(html, { cls = '', timeout = 0 } = {}) {
        const el = document.createElement('div');
        el.className = `wl-toast ${cls}`;
        el.innerHTML = html;
        toastBox.appendChild(el);
        const t = { el, timer: 0, close() { clearTimeout(t.timer); el.remove(); } };
        const wait = ms => { clearTimeout(t.timer); if (timeout) t.timer = setTimeout(t.close, ms); };
        el.addEventListener('pointerenter', () => clearTimeout(t.timer));
        el.addEventListener('focusin', () => clearTimeout(t.timer));
        el.addEventListener('focusout', e => { if (!el.contains(e.relatedTarget)) wait(2500); });
        el.addEventListener('pointerleave', () => { if (!el.contains(document.activeElement)) wait(2500); });
        wait(timeout);
        return t;
    }

    // An x to close a toast early
    const DISMISS = `<button type="button" class="wl-toast-x" data-toast="dismiss" aria-label="Dismiss">${icon('x', 16, { stroke: 2.5 })}</button>`;

    // A note: a title, an optional second line, an optional action, and an x. Notes are sentences to read, so
    // they stay up a while (Undo toasts are quicker).
    function note(title, { detail = '', action = '', onAction = null, timeout = 12000 } = {}) {
        const t = toast(`<p class="wl-toast-msg"><b class="wl-toast-title">${esc(title)}</b>${detail ? `<span class="wl-toast-detail">${esc(detail)}</span>` : ''}</p>`
            + (action ? `<button type="button" class="wl-toast-btn" data-toast="action">${esc(action)}</button>` : '') + DISMISS, { timeout });
        if (action) t.el.querySelector('[data-toast="action"]').onclick = () => { t.close(); onAction(); };
        t.el.querySelector('[data-toast="dismiss"]').onclick = t.close;
        announce(detail ? `${title}. ${detail}` : title);
        return t;
    }

    let undoToast = null;
    function offerUndo(message, undo) {
        undoToast?.close();
        const t = undoToast = toast(`<p class="wl-toast-msg">${esc(message)}</p><button type="button" class="wl-toast-btn">Undo</button>`,
            { cls: 'wl-toast-pill', timeout: 6000 });
        t.el.querySelector('button').onclick = () => { t.close(); undo(); };
        announce(`${message}. Undo is at the bottom of the screen.`);
    }

    // ---------- My Watchlist: its own page on laptops, a sheet on phones and tablets ----------
    // The address's #watchlist opens it, so Back closes it.
    const pageMQ = matchMedia('(min-width: 1024px)');
    const pageMode = () => pageMQ.matches;
    let view;
    let viewOpen = false;
    let openedHere = false; // this page added the #watchlist history entry, so closing goes Back
    let goingBack = false;  // ...and has gone Back already, waiting for the hashchange
    let pageScrollY = 0;
    const $ = selector => view.el.querySelector(selector);

    function buildView() {
        view = createLayer('wl-view', 'wl-panel', () => closeView());
        view.panel.setAttribute('aria-labelledby', 'wl-title');
        view.panel.innerHTML = `
            <div class="wl-head">
                <div class="wl-grabber" aria-hidden="true"></div>
                <button type="button" class="wl-back" data-wl="close">${icon('chevronLeft', 12, { stroke: 2.5 })}All movies</button>
                <div class="wl-titlebar">
                    <span class="wl-title-icon">${icon('bookmark', 22, { fill: 'currentColor' })}</span>
                    <p class="wl-kicker">Shared with you</p>
                    <h2 id="wl-title" tabindex="-1">My Watchlist</h2>
                    <button type="button" class="wl-close" data-wl="close" aria-label="Close">${icon('x', 22)}</button>
                    <p class="wl-summary"><span class="wl-progress"></span><span class="wl-matching"></span> <span class="wl-sep" aria-hidden="true">·</span> <button type="button" class="wl-link" data-wl="hide"></button> <span class="wl-sep" aria-hidden="true">·</span> <button type="button" class="wl-link" data-wl="save">Save a copy</button></p>
                    <p class="wl-look-summary"></p>
                    <p class="wl-made"></p>
                    <p class="wl-look-note"></p>
                    <span class="wl-hint">Drag to set the order. Tick the circle once you've watched it.</span>
                </div>
            </div>
            <div class="wl-scroll">
                <ol class="wl-rows"></ol>
                <p class="wl-empty" hidden></p>
                <div class="wl-clear-row"><button type="button" class="wl-clear" data-wl="clear">Clear list</button></div>
                <p class="wl-credit">This website uses <a href="https://www.themoviedb.org" target="_blank" rel="noopener">TMDB</a> and the TMDB APIs but is not endorsed, certified, or otherwise approved by TMDB.</p>
            </div>
            <div class="wl-look-bar"><button type="button" class="wl-btn wl-btn-blue" data-wl="add-rest">${icon('bookmarkPlus', 17, { stroke: 2.25 })}Add the rest<span class="wl-count"></span></button><button type="button" class="wl-btn" data-wl="close">Done</button></div>
            <div class="wl-sheet-tip" hidden>
                <span class="wl-tip-badge">${icon('share', 16)}</span>
                <p>${TIP_TEXT}</p>
                <button type="button" class="wl-tip-close" data-wl="tip-close" aria-label="Hide this tip">${icon('x', 16)}</button>
            </div>
            <span id="wl-grip-help" class="wl-sr">Use the up and down arrow keys to move it.</span>`;
        // On the page, it sits in the flow after the results
        document.querySelector('body > main')?.after(view.el);
        view.panel.addEventListener('click', onViewClick);
        view.panel.addEventListener('pointerdown', onViewPointerDown);
        view.panel.addEventListener('keydown', onViewKeyDown);
        dragToClose($('.wl-head'), view.panel, () => closeView(), () => !pageMode());
        pageMQ.addEventListener('change', () => { if (viewOpen) { applyMode(); renderView(); } });
    }

    function showMyWatchlist() {
        if (looking) { // from a saved list to the user's own
            endLook();
            setAddress(ownHash());
            return renderView();
        }
        if (readLink()?.open) return openView();
        openedHere = true;
        location.hash = ownHash(true).slice(1); // a history entry of its own, so Back closes it; the hashchange opens it
    }

    function openView() {
        if (!started) return;
        if (!viewOpen) {
            viewOpen = true;
            pageScrollY = scrollY;
            view.returnTo = document.activeElement;
        }
        tipToast?.close(); // the sheet has the same tip at its bottom
        renderView();
        applyMode();
    }

    function closeView() {
        if (!viewOpen) return;
        if (openedHere && readLink()?.open) { // the hashchange closes it
            if (!goingBack) { goingBack = true; history.back(); }
            return;
        }
        endLook();
        hideView();
        setAddress(ownHash());
    }

    function hideView() {
        if (!viewOpen) return;
        viewOpen = false;
        openedHere = false;
        endLook();
        closeOpenRow();
        if (openLayers.includes(confirmLayer)) closeLayer(confirmLayer); // Clear list asked about this list
        const wasPage = document.body.classList.contains('wl-page-open');
        document.body.classList.remove('wl-page-open');
        if (openLayers.includes(view)) {
            closeLayer(view); // the sheet slides away, and focus goes back
        } else {
            view.el.hidden = true;
            view.el.classList.remove('wl-shown');
            if (view.returnTo?.isConnected) view.returnTo.focus({ preventScroll: true });
        }
        if (wasPage) scrollTo(0, pageScrollY);
    }

    // Page or sheet by the window's width, again whenever it crosses over
    function applyMode() {
        const sheet = viewOpen && !pageMode();
        const page = viewOpen && pageMode();
        view.panel.setAttribute('role', sheet ? 'dialog' : 'region');
        if (sheet) view.panel.setAttribute('aria-modal', 'true');
        else view.panel.removeAttribute('aria-modal');
        if (sheet && !openLayers.includes(view)) {
            const returnTo = view.returnTo;
            openLayer(view, $('#wl-title'));
            view.returnTo = returnTo;
        }
        if (page) {
            if (openLayers.includes(view)) {
                openLayers.splice(openLayers.indexOf(view), 1);
                updateModal();
            }
            clearTimeout(view.timer);
            view.el.hidden = false;
            view.el.classList.add('wl-shown');
            if (!document.body.classList.contains('wl-page-open')) {
                document.body.classList.add('wl-page-open');
                scrollTo(0, 0);
                $('#wl-title').focus({ preventScroll: true });
            }
        }
        document.body.classList.toggle('wl-page-open', page);
    }

    // Replace the #watchlist part of the address without a new history entry
    function setAddress(hash) {
        try { history.replaceState(history.state, '', location.pathname + location.search + hash); } catch (e) {}
    }

    // ---------- The list in the address ----------
    // A copy of the user's own list rides along in the address, kept current with replaceState, so a bookmark,
    // an open tab, or an iPhone Home Screen app (which starts with its own, empty storage) can bring it back.
    // The format is permanent from launch (see the rule on shared links in index.html):
    //   #watchlist=1&backup=1&made=2026-11-09T19:41Z&m=771,10719w,13342&open=1
    //     watchlist=1  the format's version, as in a saved copy
    //     backup=1     this is the user's own list kept in the address, not a copy someone shared
    //     made         when the list last changed: ISO 8601 in UTC, to the minute
    //     m            the movies in order, by TMDB id; a trailing w marks one watched
    //     open=1       My Watchlist is open (there for a Home Screen app added with the sheet showing)
    // Opening it on an empty list restores the list, with Undo, unless it was made before Clear list last emptied
    // the list here; on a list that has movies, it's quietly replaced by that list's own copy, and it never brings
    // up the Shared with you card. An empty list has no copy. A Home Screen app ignores open=1 at launch.
    // 'all' keeps it in every browser, 'ios' only on iPhone and iPad (Safari or a Home Screen app), 'off' nowhere.
    const LIST_IN_ADDRESS = 'all';

    function listInAddress() {
        if (LIST_IN_ADDRESS === 'all') return true;
        if (LIST_IN_ADDRESS !== 'ios') return false;
        const ua = navigator.userAgent;
        const appleTouch = /iPhone|iPad|iPod/.test(ua) || (/Macintosh/.test(ua) && navigator.maxTouchPoints > 1); // iPadOS says Mac
        return navigator.standalone === true || (appleTouch && /Safari\//.test(ua) && !/CriOS|FxiOS|EdgiOS|OPiOS|GSA\//.test(ua));
    }

    const movieList = () => items.map(item => item.id + (item.watched ? 'w' : '')).join(',');

    // The address's watch-list part for the user's own list: the copy (when it's on and the list has movies),
    // or just #watchlist while My Watchlist is open, or nothing
    function ownHash(open = viewOpen && !looking) {
        if (!listInAddress() || !items.length) return open ? '#watchlist' : '';
        return `#watchlist=1&backup=1&made=${minuteISO(listMade || new Date())}&m=${movieList()}${open ? '&open=1' : ''}`;
    }

    // Bring the address up to date with the list. Not while it's showing a list someone shared (the card, or
    // Just look), and not over a hash that isn't the watch list's.
    function syncAddress() {
        if (!started || looking || pendingCopy) return;
        const link = readLink();
        if (location.hash && (!link || link.unreadable)) return; // not ours to change
        const hash = ownHash();
        if (location.hash !== hash) setAddress(hash);
    }

    // The copy in the address, back onto an empty list: a Home Screen app opening for the first time, cleared
    // storage, a private window, or someone pasting an address-bar link to a friend
    function restoreBackup(copy) {
        const had = new Set(items.map(item => item.id));
        const restored = new Set(copy.items.map(item => item.id).filter(id => !had.has(id)));
        items = cleanItems([...copy.items, ...items]);
        persistStorage();
        changed({ made: copy.made || new Date() });
        const count = shownItems().length;
        if (!count) return;
        note(`Restored your watchlist · ${plural(count, 'movie')}`, {
            action: 'Undo',
            onAction: () => { items = items.filter(item => !restored.has(item.id)); changed(); }, // keeps anything done since
            timeout: 16000,
        });
    }

    // The header's search narrows the list on the page, with the same matching as the results
    const searchInput = () => document.getElementById('search-input');
    // The page's own search normalizer (accents, apostrophes, spaces) and word-start matching, when it has them
    const searchQuery = raw => (typeof searchKey === 'function' ? searchKey(raw) : raw.trim().toLowerCase());
    function matchesSearch(id, query) {
        if (!query) return true;
        const text = movieFor(id).searchText ?? movieFor(id).title.toLowerCase();
        return typeof searchMatches === 'function' ? searchMatches(text, query) : text.includes(query);
    }

    function renderView() {
        cancelDrag?.(); // the rows are about to be redrawn
        if (!viewOpen) return;
        closeOpenRow();
        view.el.classList.toggle('wl-looking', !!looking);
        if (looking) return renderLook();
        const all = shownItems();
        const rawQuery = pageMode() ? searchInput()?.value.trim() ?? '' : '';
        const query = searchQuery(rawQuery);
        const rows = all.filter(item => !(prefs.hideWatched && item.watched) && matchesSearch(item.id, query));

        $('#wl-title').textContent = 'My Watchlist';
        $('.wl-rows').setAttribute('aria-label', 'Movies on My Watchlist');
        const watched = all.filter(item => item.watched).length;
        $('.wl-summary').hidden = !all.length;
        $('.wl-progress').textContent = `${watched} of ${all.length} watched`;
        $('.wl-matching').textContent = query ? ` · ${rows.length} ${rows.length === 1 ? 'matches' : 'match'} “${rawQuery}”` : '';
        $('[data-wl="hide"]').textContent = prefs.hideWatched ? 'Show watched' : 'Hide watched';

        const list = $('.wl-rows');
        const focus = rememberFocus(list);
        list.innerHTML = rows.map(rowHTML).join('');
        restoreFocus(list, focus);

        const empty = $('.wl-empty');
        empty.hidden = rows.length > 0;
        empty.textContent = rows.length ? ''
            : !all.length ? (pageMode() ? 'Nothing on your list yet. Point at a poster and click its bookmark to add it.'
                : 'Nothing on your list yet. Tap a poster, then Add to My Watchlist.')
            : query ? `Nothing on your list matches “${rawQuery}”.`
            : "You've watched everything on your list.";
        $('.wl-clear-row').hidden = !all.length;
        $('.wl-sheet-tip').hidden = pageMode() || !all.length || !tipApplies();
    }

    function rowHTML(item) {
        const m = movieFor(item.id);
        const title = esc(m.title);
        return `<li class="wl-row${item.watched ? ' wl-row-watched' : ''}" data-id="${item.id}">
            <button type="button" class="wl-row-delete" tabindex="-1" aria-hidden="true">${icon('trash', 16)}Remove</button>
            <div class="wl-row-main">
                <button type="button" class="wl-tick" aria-pressed="${item.watched}" aria-label="Watched: ${title}">${icon('check', 15, { stroke: 3 })}</button>
                <a class="wl-info" href="${tmdbURL(item.id)}" target="_blank" rel="noopener" draggable="false">
                    ${m.poster ? `<img src="${esc(posterAt(m.poster, 'w185'))}" alt="" loading="lazy" decoding="async" draggable="false">` : '<span class="wl-noposter"></span>'}
                    <span class="wl-text">
                        <span class="wl-name">${title} <span class="wl-year">(${yearOf(m)})</span></span>
                        <span class="wl-meta"><span class="wl-cert">${esc(m.cert)}</span><span class="wl-rating">${ratingOf(m)}</span></span>
                        <span class="wl-overview">${esc(m.overview)}</span>
                    </span>
                </a>
                <button type="button" class="wl-x" aria-label="Remove ${title} from My Watchlist">${icon('x', 15, { stroke: 2.5 })}</button>
                <button type="button" class="wl-grip" aria-label="Move ${title}" aria-describedby="wl-grip-help">${icon('grip', 20, { fill: 'currentColor', stroke: 0 })}</button>
            </div>
        </li>`;
    }

    // Keep keyboard focus on the same control across a redraw, or on its neighbor if its row went away
    function rememberFocus(list) {
        const active = document.activeElement;
        const row = active?.closest?.('.wl-row');
        if (!row || !list.contains(row)) return null;
        return { id: row.dataset.id, index: [...list.children].indexOf(row), cls: [...active.classList].find(c => c.startsWith('wl-')) };
    }

    function restoreFocus(list, focus) {
        if (!focus) return;
        const row = list.querySelector(`.wl-row[data-id="${focus.id}"]`) || list.children[Math.min(focus.index, list.children.length - 1)];
        (row?.querySelector(`.${focus.cls}`) || row?.querySelector('.wl-info') || $('#wl-title')).focus();
    }

    function onViewClick(e) {
        const action = e.target.closest('[data-wl]')?.dataset.wl;
        if (action === 'close') return closeView();
        if (action === 'hide') return toggleHideWatched();
        if (action === 'save') return saveCopy();
        if (action === 'clear') return askToClear();
        if (action === 'tip-close') return dismissTip();
        if (action === 'look-hide') return toggleLookHideWatched();
        if (action === 'add-rest') return addAllFromLook();
        if (action === 'add-one') return addOneFromLook(e.target.closest('.wl-row').dataset.id);
        const row = e.target.closest('.wl-row');
        if (looking || !row || row.classList.contains('wl-removing')) return;
        const id = row.dataset.id;
        if (e.target.closest('.wl-tick')) setWatched(id, !itemFor(id)?.watched);
        else if (e.target.closest('.wl-x')) remove(id);
        else if (e.target.closest('.wl-row-delete')) removeRow(row);
    }

    function toggleHideWatched() {
        prefs.hideWatched = !prefs.hideWatched;
        savePrefs();
        renderView();
    }

    function onViewPointerDown(e) {
        const row = e.target.closest('.wl-row');
        if (openRow && row !== openRow) closeOpenRow();
        if (looking || e.button > 0 || !row || row.classList.contains('wl-removing')) return; // a saved list is read-only
        const grip = e.target.closest('.wl-grip');
        if (grip) dragRow(e, row, grip);
        else if (!pageMode() && !e.target.closest('.wl-tick, .wl-x, .wl-row-delete')) swipeRow(e, row);
    }

    // The grip and the arrow keys move a movie up or down
    function onViewKeyDown(e) {
        const grip = e.target.closest('.wl-grip');
        if (looking || !grip || (e.key !== 'ArrowUp' && e.key !== 'ArrowDown')) return;
        e.preventDefault();
        const row = grip.closest('.wl-row');
        const order = [...row.parentElement.children].map(li => li.dataset.id);
        const from = order.indexOf(row.dataset.id);
        const to = from + (e.key === 'ArrowUp' ? -1 : 1);
        if (to < 0 || to >= order.length) return;
        order.splice(to, 0, ...order.splice(from, 1));
        reorder(order);
        announce(`${movieFor(row.dataset.id).title}: ${to + 1} of ${order.length}`);
    }

    // Drag a row by its grip. Rows trade places as it passes their middles; near the top or bottom edge, the
    // list scrolls along. A redraw of the list mid-drag (another tab's change, a row removed a moment before)
    // drops the drag, through cancelDrag.
    let cancelDrag = null;

    function dragRow(e, row, grip) {
        cancelDrag?.();
        e.preventDefault();
        const list = row.parentElement;
        const before = [...list.children].map(li => li.dataset.id).join();
        const scroller = pageMode() ? null : $('.wl-scroll');
        const scrollPos = () => (scroller ? scroller.scrollTop : scrollY);
        let base = e.clientY + scrollPos(); // where the pointer would be if the row hadn't moved
        let pointerY = e.clientY;
        let frame = 0;
        try { grip.setPointerCapture(e.pointerId); } catch (err) {}
        row.classList.add('wl-lifted');

        // The neighbors move around the row, not the row itself: taking the row out of the page, even for a
        // moment, would end the pointer capture and lose the drag
        const place = () => {
            let offset = pointerY + scrollPos() - base;
            for (let next = row.nextElementSibling; next && offset > next.offsetHeight / 2; next = row.nextElementSibling) {
                row.before(next);
                base += next.offsetHeight;
                offset -= next.offsetHeight;
                slideFrom(next, row.offsetHeight);
            }
            for (let prev = row.previousElementSibling; prev && offset < -prev.offsetHeight / 2; prev = row.previousElementSibling) {
                row.after(prev);
                base -= prev.offsetHeight;
                offset += prev.offsetHeight;
                slideFrom(prev, -row.offsetHeight);
            }
            row.style.transform = `translateY(${offset}px)`;
        };
        const autoScroll = () => {
            if (!row.isConnected) return finish(false);
            const box = scroller ? scroller.getBoundingClientRect() : { top: 0, bottom: innerHeight };
            const top = Math.max(box.top, pageMode() ? document.querySelector('body > header').getBoundingClientRect().bottom : box.top);
            const step = pointerY < top + 48 ? -Math.ceil((top + 48 - pointerY) / 6)
                : pointerY > box.bottom - 48 ? Math.ceil((pointerY - box.bottom + 48) / 6) : 0;
            if (step) {
                if (scroller) scroller.scrollTop += step;
                else scrollBy(0, step);
                place();
            }
            frame = requestAnimationFrame(autoScroll);
        };
        frame = requestAnimationFrame(autoScroll);

        const move = ev => {
            if (ev.pointerId !== e.pointerId) return;
            pointerY = ev.clientY;
            place();
        };
        // The events go to the grip while it holds the pointer and bubble up to the window, which still hears
        // them if a redraw takes the grip out of the page
        const finish = apply => {
            cancelAnimationFrame(frame);
            removeEventListener('pointermove', move);
            ['pointerup', 'pointercancel'].forEach(type => removeEventListener(type, end));
            grip.removeEventListener('lostpointercapture', end);
            cancelDrag = null;
            row.classList.remove('wl-lifted');
            row.style.transform = '';
            if (!apply || !row.isConnected) return;
            const order = [...list.children].map(li => li.dataset.id);
            if (order.join() !== before) {
                reorder(order);
                announce(`${movieFor(row.dataset.id).title}: ${order.indexOf(row.dataset.id) + 1} of ${order.length}`);
            }
        };
        const end = ev => { if (ev.pointerId === e.pointerId) finish(true); };
        cancelDrag = () => finish(false);
        addEventListener('pointermove', move);
        ['pointerup', 'pointercancel'].forEach(type => addEventListener(type, end));
        grip.addEventListener('lostpointercapture', end);
    }

    // A row that just traded places glides into its new spot
    function slideFrom(el, dy) {
        el.style.transition = 'none';
        el.style.transform = `translateY(${dy}px)`;
        void el.offsetWidth;
        el.style.transition = 'transform 0.15s ease';
        el.style.transform = '';
    }

    // Phones: swipe a row left. A short swipe shows Remove; a long one removes it; less than a nudge springs back.
    // Rows keep touch-action: pan-y, so the browser still scrolls the list natively and cancels the swipe when
    // the finger goes up or down instead.
    const REVEAL = 112; // how far a short swipe opens a row
    let openRow = null;

    function swipeRow(e, row) {
        const main = row.querySelector('.wl-row-main');
        const startX = e.clientX;
        const startY = e.clientY;
        const from = row === openRow ? -REVEAL : 0;
        let dx = from;
        let swiping = false;
        const move = ev => {
            if (ev.pointerId !== e.pointerId) return;
            const mx = ev.clientX - startX;
            const my = ev.clientY - startY;
            if (!swiping) {
                if (Math.abs(mx) < 10 && Math.abs(my) < 10) return; // not a gesture yet
                if (Math.abs(my) > Math.abs(mx)) return finish(ev); // up or down: that's scrolling
                swiping = true;
                row.classList.add('wl-swiping');
            }
            dx = Math.min(0, from + mx);
            main.style.transform = `translateX(${dx}px)`;
        };
        const finish = ev => {
            if (ev.pointerId !== e.pointerId) return;
            removeEventListener('pointermove', move);
            removeEventListener('pointerup', finish);
            removeEventListener('pointercancel', finish);
            if (!swiping) return;
            suppressClick();
            row.classList.remove('wl-swiping');
            main.style.transform = '';
            const released = ev.type === 'pointerup';
            const distance = released ? -dx : -from; // a cancelled swipe goes back to how it was
            if (distance > row.offsetWidth * 0.5) removeRow(row, dx);
            else if (distance > 56) openSwipedRow(row);
            else closeOpenRow();
        };
        addEventListener('pointermove', move);
        addEventListener('pointerup', finish);
        addEventListener('pointercancel', finish);
    }

    function openSwipedRow(row) {
        if (openRow && openRow !== row) closeOpenRow();
        openRow = row;
        row.classList.add('wl-open');
    }

    function closeOpenRow() {
        openRow?.classList.remove('wl-open');
        openRow = null;
    }

    // Slide the row away, close the gap, then take the movie off the list
    function removeRow(row, fromX) {
        row.classList.add('wl-removing');
        if (openRow === row) openRow = null;
        const main = row.querySelector('.wl-row-main');
        main.style.transition = 'none';
        main.style.transform = `translateX(${fromX ?? -REVEAL}px)`;
        void main.offsetWidth;
        main.style.transition = '';
        main.style.transform = `translateX(${-row.offsetWidth}px)`;
        row.style.height = `${row.offsetHeight}px`;
        setTimeout(() => { row.style.height = '0px'; }, 180);
        setTimeout(() => remove(row.dataset.id), 360);
    }

    // ---------- Clear list, after asking ----------
    let confirmLayer;

    function buildConfirm() {
        confirmLayer = createLayer('wl-confirm', 'wl-card wl-card-center', () => closeLayer(confirmLayer));
        confirmLayer.panel.setAttribute('aria-labelledby', 'wl-confirm-title');
        confirmLayer.panel.addEventListener('click', e => {
            const action = e.target.closest('[data-confirm]')?.dataset.confirm;
            if (action === 'save') saveCopy();
            else if (action === 'clear') { closeLayer(confirmLayer); clearList(); }
            else if (action === 'cancel') closeLayer(confirmLayer);
        });
    }

    // "this phone" on a phone, "this browser" anywhere else
    const onPhone = () => matchMedia('(pointer: coarse)').matches && Math.min(screen.width, screen.height) < 600;

    function askToClear() {
        const n = shownItems().length;
        confirmLayer.panel.innerHTML = `<h3 id="wl-confirm-title" class="wl-card-title" tabindex="-1">Clear My Watchlist?</h3>
            <p class="wl-card-text">This takes ${n === 1 ? 'the one movie' : `all ${n} movies`} off ${onPhone() ? 'this phone' : 'this browser'}’s list. Save a copy first if you might want it back later.</p>
            <button type="button" class="wl-btn wl-btn-soft" data-confirm="save">Save a copy first</button>
            <button type="button" class="wl-btn wl-btn-red" data-confirm="clear">Clear list</button>
            <button type="button" class="wl-btn wl-btn-plain" data-confirm="cancel">Cancel</button>`;
        openLayer(confirmLayer, confirmLayer.panel.querySelector('h3'));
    }

    // Clearing goes back to the main screen (the same way Done and x close My Watchlist), with focus on the
    // results and Undo at the bottom. Undo brings the list back without reopening it.
    // Cleared means cleared: the time is remembered, so a copy in the address made before it (a Home Screen
    // app's launch address, an old bookmark) doesn't bring the list back. Undo forgets that time again.
    function clearList() {
        const cleared = items;
        const clearedBefore = clearedAt;
        items = [];
        clearedAt = Date.now();
        changed();
        const results = document.getElementById('results-pane');
        if (results) {
            results.tabIndex = -1;
            view.returnTo = results;
        }
        closeView();
        offerUndo('Cleared My Watchlist', () => {
            items = cleanItems([...cleared, ...items]); // anything added since stays too
            clearedAt = clearedBefore;
            changed();
        });
    }

    // ---------- Save a copy: the whole list as a link ----------
    // Saved-copy links are permanent from day one (see the rule on shared links in index.html):
    //   #watchlist=1&made=2026-11-09T19:41Z&m=771,10719w,13342
    //     watchlist=1  the format's version. Links keep opening forever; a different kind of link gets a new number.
    //     made         when the copy was made: ISO 8601 in UTC, to the minute, with the Z
    //     m            the movies in the list's order, by TMDB id; a trailing w marks one watched
    // Read with URLSearchParams, so more keys can be added later without a new version; readers ignore keys
    // they don't know, and a missing or garbled made just means no date. (The list kept in the address is the
    // same format plus backup=1; see "The list in the address".)
    function savedCopyURL(made) {
        return `${location.origin}${location.pathname}#watchlist=1&made=${minuteISO(made)}&m=${movieList()}`;
    }

    // "Sun, Nov 9 at 2:41 PM" in the viewer's own time zone and language, with the year when it isn't this one
    function madeWhen(date) {
        const year = date.getFullYear() === new Date().getFullYear() ? {} : { year: 'numeric' };
        const day = date.toLocaleDateString(undefined, { weekday: 'short', month: 'short', day: 'numeric', ...year });
        return `${day} at ${date.toLocaleTimeString(undefined, { hour: 'numeric', minute: '2-digit' })}`;
    }

    // "Nov 9, 2:41 PM"
    const madeShort = date => `${date.toLocaleDateString(undefined, { month: 'short', day: 'numeric' })}, ${date.toLocaleTimeString(undefined, { hour: 'numeric', minute: '2-digit' })}`;

    // The share sheet on touch screens, elsewhere the clipboard. iPhone's share sheet shows only the title (cut off
    // at about 30 characters) and the site, never the link; the text goes with the link into the message.
    async function saveCopy() {
        const made = new Date();
        made.setSeconds(0, 0); // the link says the minute; show the same
        const url = savedCopyURL(made);
        const count = shownItems().length;
        if (navigator.share && matchMedia('(pointer: coarse)').matches) {
            try {
                await navigator.share({
                    title: `My Watchlist · ${madeShort(made)}`,
                    text: `My watchlist from Season's Screenings: ${plural(count, 'movie')}, made ${madeWhen(made)}`,
                    url,
                });
                return;
            } catch (e) {
                if (e.name === 'AbortError') return; // the sheet was dismissed
            }
        }
        const copied = await copyText(url);
        if (copied) note('Link copied', { detail: "Send it to whoever you're watching with, or to yourself for later. It opens this exact list.", timeout: 12000 });
        else note("Couldn't copy the link", { detail: 'Your browser blocked the clipboard.' });
    }

    async function copyText(text) {
        try {
            await navigator.clipboard.writeText(text);
            return true;
        } catch (e) {
            const area = document.createElement('textarea');
            area.value = text;
            area.setAttribute('readonly', '');
            area.style.position = 'fixed';
            area.style.opacity = '0';
            document.body.appendChild(area);
            area.select();
            let copied = false;
            try { copied = document.execCommand('copy'); } catch (err) {}
            area.remove();
            return copied;
        }
    }

    // ---------- Opening links: #watchlist, the list in the address, and saved copies ----------
    // What the address's #watchlist part asks for:
    //   { open }                  #watchlist
    //   { backup, open?, copy }   the user's own list kept in the address (backup=1)
    //   { copy }                  a saved copy someone made with Save a copy, with movies in it
    //   { unreadable }            any other #watchlist link (a later version, or garbage)
    //   null                      no #watchlist part at all
    function readLink() {
        const hash = location.hash.slice(1);
        if (!/^watchlist(?=$|[=&])/.test(hash)) return null;
        const params = new URLSearchParams(hash);
        const version = params.get('watchlist');
        if (version === '') return { open: true };
        if (version !== '1') return { unreadable: true };
        const copy = { items: parseMovies(params.get('m')), made: parseMade(params.get('made')) };
        if (params.get('backup') === '1') return { backup: true, open: params.get('open') === '1', copy };
        return copy.items.length ? { copy } : { unreadable: true };
    }

    // "771,10719w,13342" → [{ id: '771', watched: false }, { id: '10719', watched: true }, ...]; unreadable entries are skipped
    function parseMovies(text) {
        return cleanItems((text || '').split(',').map(part => {
            const match = /^\s*(\d{1,9})(w?)\s*$/i.exec(part);
            return match && { id: match[1], watched: match[2] !== '' };
        }));
    }

    // "2026-11-09T19:41Z" → a Date; missing or garbled → null
    function parseMade(text) {
        const match = /^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2})(?::\d{2}(?:\.\d+)?)?Z$/.exec(text || '');
        if (!match) return null;
        const [year, month, day, hour, minute] = match.slice(1).map(Number);
        const date = new Date(Date.UTC(year, month - 1, day, hour, minute));
        // Date.UTC rolls Feb 31 over into March; that's garbled, not a date
        return date.getUTCMonth() === month - 1 && date.getUTCDate() === day && hour < 24 && minute < 60 ? date : null;
    }

    // "2 days ago", "yesterday", "5 minutes ago": roughly how long ago, in the viewer's language
    function madeAgo(date) {
        const relative = new Intl.RelativeTimeFormat(undefined, { numeric: 'auto' });
        const minutes = Math.round((Date.now() - date) / 60000);
        if (minutes < -5) return ''; // made "in the future": a clock is off, so leave it out
        if (minutes < 1) return relative.format(0, 'second');
        const midnight = d => new Date(d.getFullYear(), d.getMonth(), d.getDate());
        const days = Math.round((midnight(new Date()) - midnight(date)) / 86400000);
        if (days < 1) return minutes < 60 ? relative.format(-minutes, 'minute') : relative.format(-Math.floor(minutes / 60), 'hour');
        if (days < 7) return relative.format(-days, 'day');
        if (days < 35) return relative.format(-Math.floor(days / 7), 'week');
        if (days < 365) return relative.format(-Math.max(1, Math.round(days / 30.4)), 'month');
        return relative.format(-Math.floor(days / 365), 'year');
    }

    // Made Sun, Nov 9 at 2:41 PM · 2 days ago
    function madeLine(date) {
        const ago = madeAgo(date);
        return `${icon('clock', 13)}<span>Made <b>${esc(madeWhen(date))}</b>${ago ? ` · ${esc(ago)}` : ''}</span>`;
    }

    // "9 movies, made Sun, Nov 9 at 2:41 PM"
    const copySummary = (count, made) => `${plural(count, 'movie')}${made ? `, made ${madeWhen(made)}` : ''}`;

    // A copy made before Clear list last emptied the list here is what was cleared, so it stays cleared. Once the
    // list has been cleared, a copy with no readable time counts as older too. (made is to the minute, so a copy
    // from the same minute as the clear counts as older.)
    const madeBeforeClear = copy => clearedAt !== null && (!copy.made || copy.made.getTime() <= clearedAt);

    // Running as an app added to the Home Screen (or installed), not in a browser tab
    const homeScreenApp = () => navigator.standalone === true || matchMedia('(display-mode: standalone)').matches;

    // On arrival (`arriving`) and on every change to the address after
    function route(arriving = false) {
        goingBack = false;
        const link = readLink();
        // A Home Screen app always launches from the address it was added with, so a saved copy there is a snapshot
        // from that moment, not something just shared: like the list in the address, it only fills an empty list
        const launchSnapshot = arriving && homeScreenApp() && link?.copy && !link.backup;
        if (link?.copy && !link.backup && !launchSnapshot) return receiveCopy(link.copy); // someone's saved copy
        if (pendingCopy) { // Back took the saved copy out of the address while its card was up
            pendingCopy = null;
            closeLayer(mergeLayer);
        }
        if (link?.unreadable) return; // nothing this version can use: ignore it, and leave it in the address
        // The list in the address comes back only on arrival (so going Back to an older address can't undo a
        // Clear list), only onto an empty list, and only if it wasn't made before the list was cleared here
        if ((link?.backup || launchSnapshot) && arriving && !shownItems().length && link.copy.items.length && !madeBeforeClear(link.copy)) {
            restoreBackup(link.copy);
        }
        // A Home Screen app always launches from the address it was added with; open=1 there would reopen My
        // Watchlist on every launch, so the app starts on the main screen (a browser tab still reopens it)
        if (link?.open && !(arriving && homeScreenApp())) {
            endLook();
            openView();
        } else if (viewOpen) {
            hideView();
            window.syncURL?.(); // Back restored an older address; the page's own state wins
        }
        syncAddress();
    }

    // Someone opened a saved copy of a list: their own from another phone or browser, or someone else's
    function receiveCopy(copy) {
        if (!shownItems().length) return restoreCopy(copy);
        if (!copy.items.some(item => movieFor(item.id))) return setAddress(ownHash());
        if (history.state?.wlLook) return startLook(copy); // reloaded while just looking
        showMergeCard(copy);
    }

    // Onto an empty list it comes back exactly as saved, watched marks and all (and any movies the data
    // doesn't have, kept out of sight as usual)
    function restoreCopy(copy) {
        items = cleanItems([...copy.items, ...items]);
        persistStorage();
        setAddress(ownHash());
        changed();
        const count = shownItems().length;
        if (count) note('Restored your watchlist', { detail: copySummary(count, copy.made), action: 'Open', onAction: showMyWatchlist, timeout: 16000 });
    }

    // Movies brought in from a saved list (Add N new on the card; Add the rest, or one at a time, while just
    // looking) come in unwatched, whatever the saved list says. Set to true to bring its watched marks along.
    // The marks on movies already on the user's list never change either way.
    const NEW_MOVIES_KEEP_SAVED_WATCHED_MARKS = false;

    // Add the movies the user doesn't have yet, after their own, in the saved order. Returns how many.
    function addFromCopy(list) {
        const fresh = list.filter(item => movieFor(item.id) && !itemFor(item.id))
            .map(item => ({ id: item.id, watched: NEW_MOVIES_KEEP_SAVED_WATCHED_MARKS && item.watched }));
        if (!fresh.length) return 0;
        if (!items.length) persistStorage();
        items = cleanItems([...items, ...fresh]);
        changed();
        return fresh.length;
    }

    // ----- The card for a saved list when the user already has one: Add N new, Just look, or No thanks -----
    let mergeLayer;
    let pendingCopy = null;

    function buildMergeCard() {
        mergeLayer = createLayer('wl-merge', 'wl-card', declineCopy);
        mergeLayer.panel.setAttribute('aria-labelledby', 'wl-merge-title');
        mergeLayer.panel.addEventListener('click', e => {
            const action = e.target.closest('[data-merge]')?.dataset.merge;
            if (action === 'add') addAllFromCard();
            else if (action === 'look') lookFromCard();
            else if (action === 'no') declineCopy();
        });
    }

    function showMergeCard(copy) {
        pendingCopy = copy;
        const known = copy.items.filter(item => movieFor(item.id));
        const fresh = known.filter(item => !itemFor(item.id)).length;
        const watched = known.filter(item => item.watched).length;
        const posters = known.slice(0, 6).map(item => posterAt(movieFor(item.id).poster, 'w154'));
        const more = known.length - posters.length;
        // A line about the link itself (when it was made) sits under the heading, like the restore note's detail
        mergeLayer.panel.innerHTML = `
            <p class="wl-kicker">Shared with you</p>
            <h3 id="wl-merge-title" class="wl-card-title" tabindex="-1">${plural(known.length, 'movie')}, ${watched} watched</h3>
            ${copy.made ? `<p class="wl-made">${madeLine(copy.made)}</p>` : ''}
            <div class="wl-strip" aria-hidden="true">${posters.map(src => (src ? `<img src="${esc(src)}" alt="" decoding="async">` : '<span class="wl-noposter"></span>')).join('')}${more > 0 ? `<span class="wl-strip-more">+${more}</span>` : ''}</div>
            <p class="wl-card-text">You already have ${plural(shownItems().length, 'movie')} on your list. ${fresh ? `${fresh} of these ${fresh === 1 ? 'is' : 'are'} new to it.` : 'All of these are on it already.'}</p>
            ${fresh ? `<button type="button" class="wl-btn wl-btn-blue" data-merge="add">${icon('bookmarkPlus', 17, { stroke: 2.25 })}Add ${fresh} new</button>` : ''}
            <div class="wl-card-pair">
                <button type="button" class="wl-btn wl-btn-soft" data-merge="look">Just look</button>
                <button type="button" class="wl-btn" data-merge="no">${fresh ? 'No thanks' : 'Done'}</button>
            </div>`;
        openLayer(mergeLayer, mergeLayer.panel.querySelector('h3'));
    }

    function addAllFromCard() {
        const added = addFromCopy(pendingCopy.items);
        pendingCopy = null;
        closeLayer(mergeLayer);
        setAddress(ownHash());
        if (added) note(`Added ${plural(added, 'movie')} to My Watchlist`, { action: 'Open', onAction: showMyWatchlist, timeout: 12000 });
    }

    function lookFromCard() {
        const copy = pendingCopy;
        pendingCopy = null;
        closeLayer(mergeLayer);
        startLook(copy);
    }

    function declineCopy() {
        pendingCopy = null;
        closeLayer(mergeLayer);
        setAddress(ownHash());
    }

    // ----- Just look: the saved list in My Watchlist's place, read-only, without touching the user's -----
    // The saved link stays in the address (with a note in the history entry), so a reload comes back to it.
    let looking = null; // the saved list on show: { items, made }
    let lookHideWatched = false;

    function startLook(copy) {
        looking = copy;
        lookHideWatched = false;
        $('[data-wl="add-rest"] .wl-count').textContent = ''; // a new list: no bounce for its first count
        history.replaceState({ ...(history.state || {}), wlLook: true }, '');
        if (viewOpen) renderView();
        else openView();
    }

    function endLook() {
        if (!looking) return;
        looking = null;
        const state = { ...(history.state || {}) };
        delete state.wlLook;
        history.replaceState(state, '');
    }

    // The saved list's rows that are showing: watched ones can be hidden, and on the page the search narrows them
    function lookRows() {
        const query = pageMode() ? searchQuery(searchInput()?.value ?? '') : '';
        return looking.items.filter(item => movieFor(item.id) && !(lookHideWatched && item.watched) && matchesSearch(item.id, query));
    }

    function renderLook() {
        const known = looking.items.filter(item => movieFor(item.id));
        const watched = known.filter(item => item.watched).length;
        const rows = lookRows();
        const fresh = rows.filter(item => !itemFor(item.id)).length; // the add button adds what's showing

        $('#wl-title').textContent = `${plural(known.length, 'movie')}, ${watched} watched`;
        $('.wl-look-summary').innerHTML = watched || lookHideWatched
            ? `<button type="button" class="wl-link" data-wl="look-hide">${lookHideWatched ? 'Show watched' : 'Hide watched'}</button>` : '';
        $('.wl-made').innerHTML = looking.made ? madeLine(looking.made) : '';
        $('.wl-look-note').textContent = `${mouseDevice.matches ? 'Click' : 'Tap'} a bookmark to add to your list.`;

        const list = $('.wl-rows');
        list.setAttribute('aria-label', 'Movies on the saved watchlist');
        const focus = rememberFocus(list);
        list.innerHTML = rows.map(lookRowHTML).join('');
        restoreFocus(list, focus);
        $('.wl-empty').hidden = rows.length > 0;
        $('.wl-empty').textContent = rows.length ? '' : 'Nothing to show here with watched movies hidden.';

        // Add the rest, with a count that keeps up as bookmarks are tapped or watched ones hidden
        const addButton = $('[data-wl="add-rest"]');
        const counter = addButton.querySelector('.wl-count');
        if (counter.textContent !== String(fresh)) {
            if (counter.textContent) replay(counter, 'wl-bump');
            counter.textContent = fresh;
        }
        addButton.hidden = !fresh;
        addButton.setAttribute('aria-label', `Add the rest: ${plural(fresh, 'movie')}`);
    }

    // Read-only: the saved list's watched mark, and a bookmark that's solid if the movie is already on the
    // user's list, or adds it if not
    function lookRowHTML(item) {
        const m = movieFor(item.id);
        const title = esc(m.title);
        const mark = itemFor(item.id)
            ? `<span class="wl-onlist" role="img" aria-label="Already on your list" title="Already on your list">${icon('bookmark', 15, { fill: 'currentColor' })}</span>`
            : `<button type="button" class="wl-addone" data-wl="add-one" aria-label="Add ${title} to My Watchlist" title="Add to My Watchlist">${icon('bookmarkPlus', 15, { stroke: 2.25 })}</button>`;
        return `<li class="wl-row${item.watched ? ' wl-row-watched' : ''}" data-id="${item.id}">
            <div class="wl-row-main">
                <span class="wl-tick wl-tick-static" role="img" aria-label="${item.watched ? 'Watched' : 'Not watched'}">${icon('check', 15, { stroke: 3 })}</span>
                <a class="wl-info" href="${tmdbURL(item.id)}" target="_blank" rel="noopener" draggable="false">
                    ${m.poster ? `<img src="${esc(posterAt(m.poster, 'w185'))}" alt="" loading="lazy" decoding="async" draggable="false">` : '<span class="wl-noposter"></span>'}
                    <span class="wl-text">
                        <span class="wl-name">${title} <span class="wl-year">(${yearOf(m)})</span></span>
                        <span class="wl-meta"><span class="wl-cert">${esc(m.cert)}</span><span class="wl-rating">${ratingOf(m)}</span></span>
                        <span class="wl-overview">${esc(m.overview)}</span>
                    </span>
                </a>
                ${mark}
            </div>
        </li>`;
    }

    function toggleLookHideWatched() {
        lookHideWatched = !lookHideWatched;
        renderView();
    }

    // A row's bookmark adds that movie right away (it turns solid and the count bounces), with Undo
    function addOneFromLook(id) {
        const item = looking?.items.find(entry => entry.id === id);
        if (!item || !addFromCopy([item])) return;
        offerUndo(`Added ${movieFor(id).title}`, () => {
            items = items.filter(entry => entry.id !== id);
            changed();
        });
    }

    // Add the rest: bring in every movie showing that the user doesn't have, then show their own list with them at the end
    function addAllFromLook() {
        const added = addFromCopy(lookRows());
        endLook();
        setAddress(ownHash());
        renderView();
        $('#wl-title').focus({ preventScroll: true });
        if (added) note(`Added ${plural(added, 'movie')} to My Watchlist`);
    }

    // ---------- The Home Screen tip, iPhone Safari only ----------
    // Add to Home Screen lives in Safari's Share menu, not behind the page's Share results, so the tip names it
    // with Safari's own icon. It pops up once, right after the very first add, and sits at the bottom of the list
    // sheet; closing either one closes both for good. Never shown once the site is on the Home Screen.
    const TIP_TEXT = `Add Season’s Screenings to your Home Screen: open Safari’s Share menu ${icon('share', 13, { stroke: 2.25, label: 'Share', cls: 'wl-inline-icon' })} and choose <b>Add to Home Screen</b>.`;
    let tipToast = null;
    // Off for now: iPhone saves the address the page was loaded with, so a Home Screen app added after building a
    // list would start empty, and Safari and the app would keep separate lists. See DESIGN.md, "Add to Home Screen".
    const HOME_SCREEN_TIP = false;

    function tipApplies() {
        const ua = navigator.userAgent;
        return HOME_SCREEN_TIP && !prefs.tipDone && !homeScreenApp() && /iPhone/.test(ua) && /Safari\//.test(ua) && !/CriOS|FxiOS|EdgiOS|OPiOS|GSA\//.test(ua);
    }

    function firstAddTip(title) {
        if (prefs.tipShown || !tipApplies()) return;
        prefs.tipShown = true;
        savePrefs();
        tipToast = toast(`<p class="wl-toast-msg"><b class="wl-toast-title">Added ${esc(title)}.</b>${TIP_TEXT}</p>`
            + `<button type="button" class="wl-toast-x" aria-label="Dismiss">${icon('x', 16, { stroke: 2.5 })}</button>`, { cls: 'wl-toast-tip', timeout: 40000 });
        tipToast.el.querySelector('.wl-toast-x').onclick = dismissTip;
    }

    function dismissTip() {
        prefs.tipDone = true;
        savePrefs();
        tipToast?.close();
        tipToast = null;
        renderView();
    }

    // ---------- Styles ----------
    const CSS = `
        /* The filters' My Watchlist section: room for the fold button when it's the first heading */
        @media (max-width: 1023px) {
            #level-section.hidden + #wl-filters h3, #level-section.hidden + #wl-filters.hidden + div h3 { padding-right: 30px; }
        }

        /* ----- Result cards: the bookmark at the top right, the Watched ribbon across the top left ----- */
        .wl-poster { container-type: inline-size; }
        .wl-mark {
            position: absolute; top: 5px; right: 5px; z-index: 2; width: 24px; height: 24px; box-sizing: border-box; padding: 0;
            display: flex; align-items: center; justify-content: center; border: 0; border-radius: 999px; cursor: pointer;
            background: #2563eb; color: #ffffff; box-shadow: 0 1px 4px rgba(15,23,42,0.35); opacity: 0; transition: opacity 0.15s;
        }
        .wl-mark svg { width: 54%; height: 54%; transition: transform 0.15s; }
        .wl-mark .wl-mark-off, .movie-card:not(.wl-on) .wl-mark .wl-mark-on { display: none; }
        .movie-card:not(.wl-on) .wl-mark .wl-mark-off { display: block; }
        .movie-card:not(.wl-on) .wl-mark { background: #ffffff; color: #2563eb; border: 2px solid #3b82f6; box-shadow: 0 2px 8px rgba(15,23,42,0.35); }
        .movie-card.wl-on .wl-mark, .wl-mark:focus-visible { opacity: 1; }
        .wl-mark:focus-visible { outline: 2px solid #1d4ed8; outline-offset: 2px; }
        .wl-mark-label {
            display: none; position: absolute; right: calc(100% + 6px); top: 50%; transform: translateY(-50%); pointer-events: none;
            padding: 4px 8px; border-radius: 6px; background: #0f172a; color: #ffffff; font-size: 11px; font-weight: 600; line-height: 1.3; white-space: nowrap;
        }
        .wl-mark:focus-visible .wl-mark-label { display: block; }
        /* A mouse can point: the bookmark and its label appear over the poster under it */
        @media (hover: hover) and (pointer: fine) {
            .movie-card:hover .wl-mark { opacity: 1; }
            .wl-mark:hover svg { transform: scale(1.12); }
            .movie-card:not(.wl-on):hover .wl-mark-label, .wl-mark:hover .wl-mark-label { display: block; }
        }
        .wl-ribbon {
            display: none; position: absolute; top: 13px; left: -30px; width: 109px; z-index: 1; transform: rotate(-45deg); pointer-events: none;
            padding: 4px 0; background: #16a34a; color: #ffffff; text-align: center; box-shadow: 0 2px 6px rgba(0,0,0,0.35);
            font-size: 8px; line-height: 1.2; font-weight: 900; text-transform: uppercase; letter-spacing: 0.12em;
        }
        @media (min-width: 1024px) {
            .wl-mark { top: 8px; right: 8px; width: 32px; height: 32px; }
            .wl-ribbon { top: 26px; left: -54px; width: 220px; padding: 6px 0; font-size: 10px; }
        }
        /* Sized to the poster, from small phone tiles to big laptop cards */
        @supports (width: 1cqw) {
            .wl-mark { width: clamp(24px, calc(16px + 7.7cqw), 32px); height: clamp(24px, calc(16px + 7.7cqw), 32px); top: clamp(5px, 4cqw, 8px); right: clamp(5px, 4cqw, 8px); }
            .wl-ribbon {
                top: 0; left: 0; width: 110cqw; padding: clamp(4px, 2.9cqw, 6px) 0; font-size: clamp(8px, 6.5cqw, 10px);
                transform: translate(-50%, -50%) translate(23cqw, 23cqw) rotate(-45deg);
            }
        }
        .movie-card.wl-watched .wl-ribbon { display: block; }
        .movie-card.wl-watched .wl-poster img { opacity: 0.45; filter: grayscale(0.5); }
        .movie-card.wl-watched .card-body { opacity: 0.5; }
        @keyframes wl-pop { 0% { transform: scale(0.6); } 60% { transform: scale(1.2); } 100% { transform: scale(1); } }
        .wl-mark.wl-pop { animation: wl-pop 0.35s ease-out; }

        /* ----- Layers ----- */
        html.wl-locked, html.wl-locked body { overflow: hidden !important; }
        #results-pane:focus { outline: none; } /* where focus lands after Clear list */
        .wl-layer { position: fixed; inset: 0; z-index: 80; color: #0f172a; }
        .wl-layer[hidden] { display: none; }
        .wl-backdrop { position: absolute; inset: 0; background: rgba(15,23,42,0.4); opacity: 0; transition: opacity 0.25s; touch-action: none; }
        .wl-layer.wl-shown .wl-backdrop { opacity: 1; }
        .wl-layer section:focus, .wl-layer h2:focus, .wl-layer h3:focus { outline: none; }
        .wl-grabber { width: 36px; height: 5px; border-radius: 3px; background: #e2e8f0; margin: 0 auto; }
        .wl-year { color: #94a3b8; font-weight: 400; }
        .wl-meta { display: flex; align-items: center; gap: 8px; margin-top: 4px; }
        .wl-cert { color: #2563eb; font-weight: 900; font-size: 10px; letter-spacing: -0.05em; border: 1px solid #dbeafe; padding: 0 4px; border-radius: 4px; }
        .wl-rating { color: #94a3b8; font-size: 10px; font-weight: 700; font-style: italic; }
        .wl-noposter { display: block; background: #f1f5f9; }
        .wl-btn {
            height: 46px; width: 100%; box-sizing: border-box; padding: 0 12px; display: flex; align-items: center; justify-content: center; gap: 8px;
            border: 1px solid #e2e8f0; border-radius: 12px; background: #ffffff; color: #475569; cursor: pointer; text-decoration: none;
            font: inherit; font-size: 11px; font-weight: 800; text-transform: uppercase; letter-spacing: 0.08em; white-space: nowrap;
        }
        .wl-btn:hover { border-color: #cbd5e1; }
        .wl-btn-blue { border: 0; background: #2563eb; color: #ffffff; }
        .wl-btn-blue:hover { background: #1d4ed8; }
        .wl-btn-green { border: 0; background: #16a34a; color: #ffffff; }
        .wl-btn-green:hover { background: #15803d; }
        .wl-btn-soft { border-color: #bfdbfe; background: #eff6ff; color: #2563eb; }
        .wl-btn-red { border: 0; background: #ef4444; color: #ffffff; }
        .wl-btn-plain { border: 0; background: none; color: #64748b; height: 40px; }

        /* ----- The peek: a bottom sheet ----- */
        #wl-peek .wl-backdrop { background: rgba(15,23,42,0.45); }
        .wl-peek-sheet {
            position: absolute; left: 0; right: 0; bottom: 0; max-width: 560px; margin: 0 auto; box-sizing: border-box;
            padding: 8px 20px calc(30px + env(safe-area-inset-bottom, 0px)); background: #ffffff; border-radius: 18px 18px 0 0;
            box-shadow: 0 -10px 30px rgba(15,23,42,0.25); touch-action: none; -webkit-user-select: none; user-select: none;
            transform: translateY(100%); transition: transform 0.3s cubic-bezier(0.2, 0.8, 0.2, 1);
        }
        #wl-peek.wl-shown .wl-peek-sheet { transform: none; }
        .wl-peek-sheet .wl-grabber { margin-bottom: 16px; }
        .wl-peek-top { display: flex; gap: 14px; align-items: flex-start; }
        .wl-peek-top img, .wl-peek-top .wl-noposter { flex: none; width: 86px; height: 129px; object-fit: cover; border-radius: 8px; box-shadow: 0 4px 12px rgba(15,23,42,0.25); }
        .wl-peek-text { min-width: 0; }
        .wl-peek-status { display: inline-flex; align-items: center; gap: 5px; margin-bottom: 6px; color: #2563eb; font-size: 9px; font-weight: 800; text-transform: uppercase; letter-spacing: 0.1em; }
        .wl-peek-text h3 { margin: 0; font-size: 18px; font-weight: 800; line-height: 1.2; color: #0f172a; }
        .wl-peek-text .wl-meta { margin-top: 6px; }
        .wl-peek-text .wl-rating { font-size: 11px; }
        .wl-peek-overview {
            margin: 8px 0 0; font-size: 12.5px; line-height: 1.45; color: #475569;
            display: -webkit-box; -webkit-line-clamp: 6; -webkit-box-orient: vertical; overflow: hidden;
        }
        .wl-peek-add { margin-top: 18px; box-shadow: 0 6px 16px -6px rgba(37,99,235,0.6); }
        .wl-peek-pair { display: grid; grid-template-columns: 1fr 1fr; gap: 10px; margin-top: 18px; }
        .wl-btn-tmdb { margin-top: 10px; height: 42px; font-weight: 700; color: #334155; }
        .wl-btn-tmdb svg { color: #64748b; }
        @media (orientation: landscape) and (max-height: 500px) {
            .wl-peek-sheet { padding-bottom: calc(14px + env(safe-area-inset-bottom, 0px)); }
            .wl-peek-top img, .wl-peek-top .wl-noposter { width: 64px; height: 96px; }
            .wl-peek-overview { -webkit-line-clamp: 3; }
            .wl-peek-add, .wl-peek-pair { margin-top: 12px; }
        }

        /* ----- The My List buttons ----- */
        #wl-fab, #wl-header-button {
            display: none; align-items: center; gap: 8px; padding: 0 8px 0 14px; border: 0; background: #2563eb; color: #ffffff;
            font: inherit; cursor: pointer;
        }
        #wl-fab {
            position: fixed; z-index: 60; right: 12px; bottom: calc(20px + env(safe-area-inset-bottom, 0px)); height: 44px; border-radius: 999px;
            box-shadow: 0 10px 24px -6px rgba(37,99,235,0.55), 0 4px 10px rgba(15,23,42,0.18);
        }
        #wl-header-button { flex: none; height: 48px; border-radius: 12px; box-shadow: 0 4px 12px -4px rgba(37,99,235,0.5); }
        #wl-fab:hover, #wl-header-button:hover, body.wl-page-open #wl-header-button { background: #1d4ed8; }
        .wl-fab-label { font-size: 11px; font-weight: 700; text-transform: uppercase; letter-spacing: 0.1em; white-space: nowrap; }
        #wl-header-button .wl-fab-label { font-size: 10px; }
        .wl-count {
            min-width: 24px; height: 24px; padding: 0 6px; box-sizing: border-box; border-radius: 999px; background: #ffffff; color: #2563eb;
            font-size: 12px; font-weight: 900; display: flex; align-items: center; justify-content: center;
        }
        #wl-header-button .wl-count { font-size: 11px; }
        @keyframes wl-bump { 0% { transform: scale(1); } 40% { transform: scale(1.4); } 100% { transform: scale(1); } }
        .wl-count.wl-bump { animation: wl-bump 0.4s ease-out; }
        @media (max-width: 1023px) {
            body.wl-has-list #wl-fab { display: flex; }
            /* Hidden while a sheet or card is open; sideways, the sheet is narrower and the button showed beside it */
            body.wl-modal-open #wl-fab { visibility: hidden; }
            /* Room under the last row of results for the floating button */
            body.wl-has-list #results-pane { padding-bottom: calc(84px + env(safe-area-inset-bottom, 0px)); }
        }
        @media (max-width: 1023px) and (orientation: landscape) and (max-height: 500px) {
            #wl-fab { right: 16px; bottom: calc(14px + env(safe-area-inset-bottom, 0px)); }
        }
        @media (min-width: 1024px) {
            body.wl-has-list #wl-header-button { display: flex; }
        }
        /* Narrow laptops: just the bookmark and the count, so the search keeps its room */
        @media (min-width: 1024px) and (max-width: 1279px) {
            #wl-header-button .wl-fab-label { display: none; }
            #wl-header-button { padding-left: 12px; }
        }

        /* ----- Toasts ----- */
        #wl-toasts {
            position: fixed; z-index: 100; left: 12px; right: 12px; bottom: calc(16px + env(safe-area-inset-bottom, 0px));
            display: flex; flex-direction: column; align-items: center; gap: 8px; pointer-events: none;
        }
        @media (max-width: 1023px) {
            body.wl-has-list:not(.wl-modal-open) #wl-toasts { bottom: calc(76px + env(safe-area-inset-bottom, 0px)); } /* above My List */
        }
        @media (max-width: 1023px) and (orientation: landscape) and (max-height: 500px) {
            body.wl-has-list:not(.wl-modal-open) #wl-toasts { bottom: calc(70px + env(safe-area-inset-bottom, 0px)); }
        }
        @media (min-width: 1024px) { #wl-toasts { bottom: 24px; } }
        .wl-toast {
            pointer-events: auto; position: relative; box-sizing: border-box; width: 100%; max-width: 440px; display: flex; align-items: center; gap: 10px;
            padding: 10px 8px 10px 14px; border-radius: 12px; background: #0f172a; color: #e2e8f0; box-shadow: 0 10px 30px rgba(15,23,42,0.35);
            font-size: 12px; line-height: 1.45; animation: wl-toast-in 0.2s ease-out;
        }
        @keyframes wl-toast-in { from { opacity: 0; transform: translateY(8px); } }
        .wl-toast-msg { flex: 1; min-width: 0; margin: 0; }
        .wl-toast-msg b { color: #ffffff; }
        .wl-toast-title, .wl-toast-detail { display: block; }
        .wl-toast-title { margin-bottom: 1px; }
        .wl-toast-x { flex: none; width: 32px; height: 32px; padding: 0; display: flex; align-items: center; justify-content: center; border: 0; background: none; color: #94a3b8; cursor: pointer; }
        .wl-inline-icon { display: inline-block; vertical-align: -2px; } /* Tailwind makes every svg a block */
        /* The Home Screen tip points down at the My List button */
        .wl-toast-tip { align-self: flex-end; padding: 10px 6px 10px 14px; }
        .wl-toast-tip::after { content: ''; position: absolute; right: 52px; bottom: -6px; width: 12px; height: 12px; background: #0f172a; transform: rotate(45deg); }
        .wl-toast-pill { min-height: 48px; padding: 6px 6px 6px 18px; border-radius: 999px; }
        .wl-toast-pill .wl-toast-msg { font-size: 13px; font-weight: 600; color: #ffffff; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
        .wl-toast-btn {
            flex: none; height: 36px; padding: 0 14px; border: 0; border-radius: 999px; background: rgba(255,255,255,0.12); color: #93c5fd; cursor: pointer;
            font: inherit; font-size: 11px; font-weight: 800; text-transform: uppercase; letter-spacing: 0.1em;
        }
        .wl-toast-btn:hover { background: rgba(255,255,255,0.2); }
        .wl-sr { position: absolute; width: 1px; height: 1px; overflow: hidden; clip-path: inset(50%); white-space: nowrap; }

        /* ----- My Watchlist: a sheet on phones and tablets ----- */
        .wl-panel {
            position: absolute; left: 0; right: 0; top: 44px; bottom: 0; max-width: 640px; margin: 0 auto; display: flex; flex-direction: column;
            overflow: hidden; background: #ffffff; border-radius: 16px 16px 0 0; box-shadow: 0 -10px 30px rgba(15,23,42,0.25);
            transform: translateY(100%); transition: transform 0.3s cubic-bezier(0.2, 0.8, 0.2, 1);
        }
        #wl-view.wl-shown .wl-panel { transform: none; }
        @media (orientation: landscape) and (max-height: 500px) { .wl-panel { top: 8px; } }
        .wl-head { flex: none; padding: 8px 16px 10px; border-bottom: 1px solid #e2e8f0; touch-action: none; -webkit-user-select: none; user-select: none; }
        .wl-head .wl-grabber { margin-bottom: 8px; }
        .wl-back, .wl-hint, .wl-credit { display: none; }
        .wl-titlebar { display: flex; flex-wrap: wrap; align-items: center; column-gap: 8px; }
        .wl-title-icon { display: flex; color: #2563eb; }
        .wl-titlebar h2 { flex: 1; margin: 0; font-size: 20px; font-weight: 900; letter-spacing: -0.05em; text-transform: uppercase; color: #0f172a; }
        .wl-close {
            flex: none; width: 44px; height: 44px; margin: -6px -10px -6px 0; padding: 0; border: 0; background: none; color: #94a3b8; cursor: pointer;
            display: flex; align-items: center; justify-content: center;
        }
        .wl-summary { flex-basis: 100%; margin: 2px 0 0; font-size: 12px; color: #64748b; }
        .wl-summary[hidden] { display: none; }
        .wl-sep { color: #cbd5e1; }
        .wl-link { padding: 0; border: 0; background: none; font: inherit; font-weight: 700; color: #2563eb; cursor: pointer; }
        .wl-link:hover { color: #1d4ed8; }
        .wl-scroll { flex: 1; min-height: 0; overflow-y: auto; overscroll-behavior: contain; padding-bottom: env(safe-area-inset-bottom, 0px); }
        .wl-rows { list-style: none; margin: 0; padding: 0; }
        .wl-row { position: relative; overflow: hidden; border-bottom: 1px solid #f1f5f9; background: #ffffff; transition: height 0.18s ease; }
        .wl-row-delete {
            position: absolute; inset: 0; display: flex; align-items: center; justify-content: flex-end; gap: 6px; padding: 0 22px 0 0; border: 0;
            background: #ef4444; color: #ffffff; font: inherit; font-size: 11px; font-weight: 700; text-transform: uppercase; letter-spacing: 0.05em; cursor: pointer;
        }
        .wl-row:not(.wl-swiping):not(.wl-open):not(.wl-removing) .wl-row-delete { visibility: hidden; }
        .wl-row-main {
            position: relative; display: flex; align-items: center; gap: 10px; padding: 9px 4px 9px 16px; background: #ffffff;
            touch-action: pan-y; -webkit-user-select: none; user-select: none; transition: transform 0.2s ease;
        }
        .wl-row.wl-swiping .wl-row-main { transition: none; }
        .wl-row.wl-open .wl-row-main { transform: translateX(-112px); }
        .wl-row.wl-open .wl-row-main, .wl-row.wl-swiping .wl-row-main, .wl-row.wl-removing .wl-row-main { box-shadow: 6px 0 12px -6px rgba(15,23,42,0.25); }
        .wl-row.wl-lifted { z-index: 2; overflow: visible; box-shadow: 0 12px 28px -8px rgba(15,23,42,0.35); }
        .wl-row.wl-lifted .wl-row-main { background: #f8fafc; }
        .wl-tick {
            flex: none; width: 28px; height: 28px; box-sizing: border-box; padding: 0; display: flex; align-items: center; justify-content: center;
            border: 2px solid #cbd5e1; border-radius: 999px; background: #ffffff; color: transparent; cursor: pointer;
        }
        .wl-tick:hover { border-color: #16a34a; }
        .wl-tick[aria-pressed="true"], .wl-row-watched .wl-tick-static { border: 0; background: #16a34a; color: #ffffff; }
        .wl-info { flex: 1; min-width: 0; display: flex; align-items: center; gap: 10px; color: inherit; text-decoration: none; -webkit-user-drag: none; }
        .wl-row-watched .wl-info { opacity: 0.5; }
        .wl-info img, .wl-info .wl-noposter { flex: none; width: 40px; height: 60px; object-fit: cover; border-radius: 6px; background: #f1f5f9; }
        .wl-text { display: block; min-width: 0; }
        .wl-name { display: block; font-size: 13px; font-weight: 700; line-height: 1.25; color: #1e293b; }
        .wl-text .wl-meta { display: flex; }
        .wl-overview { display: none; }
        .wl-x {
            flex: none; width: 32px; height: 32px; padding: 0; display: flex; align-items: center; justify-content: center;
            border: 0; border-radius: 8px; background: #f1f5f9; color: #64748b; cursor: pointer;
        }
        .wl-x:hover { background: #e2e8f0; color: #334155; }
        .wl-grip {
            flex: none; width: 36px; height: 44px; margin: -8px 0; padding: 0; display: flex; align-items: center; justify-content: center;
            border: 0; background: none; color: #cbd5e1; cursor: grab; touch-action: none;
        }
        .wl-grip:hover { color: #94a3b8; }
        .wl-lifted .wl-grip { cursor: grabbing; color: #64748b; }
        /* A mouse gets the x on the row it points at; touch screens swipe, so there it stays out of sight
           (but not out of reach of a keyboard or screen reader) */
        @media (hover: hover) and (pointer: fine) {
            .wl-row:not(:hover) .wl-x:not(:focus-visible) { opacity: 0; }
        }
        @media (max-width: 1023px) and (hover: none), (max-width: 1023px) and (pointer: coarse) {
            .wl-x:not(:focus-visible) { position: absolute; width: 1px; height: 1px; overflow: hidden; clip-path: inset(50%); }
        }
        .wl-empty { margin: 0; padding: 36px 24px; text-align: center; font-size: 13px; line-height: 1.5; color: #64748b; }
        .wl-empty[hidden], .wl-clear-row[hidden] { display: none; }
        .wl-clear-row { padding: 18px 0; text-align: center; }
        .wl-clear {
            padding: 6px 10px; border: 0; background: none; color: #94a3b8; cursor: pointer;
            font: inherit; font-size: 11px; font-weight: 700; text-transform: uppercase; letter-spacing: 0.08em;
        }
        .wl-clear:hover { color: #ef4444; }

        /* The Home Screen tip at the bottom of the sheet */
        .wl-sheet-tip { flex: none; display: flex; align-items: center; gap: 10px; padding: 12px 6px calc(12px + env(safe-area-inset-bottom, 0px)) 16px; border-top: 1px solid #e2e8f0; background: #ffffff; }
        .wl-sheet-tip[hidden] { display: none; }
        .wl-sheet-tip p { flex: 1; margin: 0; font-size: 11px; line-height: 1.35; color: #64748b; }
        .wl-sheet-tip b { color: #334155; }
        .wl-tip-badge { flex: none; width: 32px; height: 32px; display: flex; align-items: center; justify-content: center; border-radius: 8px; background: #eff6ff; color: #2563eb; }
        .wl-tip-close { flex: none; width: 36px; height: 36px; padding: 0; display: flex; align-items: center; justify-content: center; border: 0; background: none; color: #94a3b8; cursor: pointer; }

        /* ----- Just looking at a saved list: read-only, in My Watchlist's place ----- */
        .wl-kicker, .wl-look-summary, .wl-made, .wl-look-note, .wl-look-bar { display: none; }
        .wl-kicker { margin: 0; font-size: 10px; font-weight: 800; color: #2563eb; text-transform: uppercase; letter-spacing: 0.1em; }
        .wl-made { align-items: center; gap: 6px; margin: 5px 0 0; font-size: 12px; color: #475569; }
        .wl-made svg { flex: none; color: #64748b; }
        .wl-made b { color: #1e293b; }
        /* Shared with you / 9 movies, 3 watched  Hide watched / Made ... / Tap a bookmark ... */
        #wl-view.wl-looking .wl-titlebar { position: relative; align-items: baseline; column-gap: 10px; padding-right: 36px; }
        #wl-view.wl-looking .wl-close { position: absolute; top: -6px; right: -10px; margin: 0; }
        #wl-view.wl-looking .wl-kicker, #wl-view.wl-looking .wl-look-note { display: block; flex-basis: 100%; }
        #wl-view.wl-looking .wl-look-summary { display: block; }
        #wl-view.wl-looking .wl-made:not(:empty) { display: flex; flex-basis: 100%; }
        #wl-view.wl-looking .wl-titlebar h2 { flex: none; margin-top: 4px; font-size: 19px; font-weight: 800; letter-spacing: -0.02em; text-transform: none; }
        #wl-view.wl-looking :is(.wl-title-icon, .wl-summary, .wl-hint, .wl-clear-row, .wl-sheet-tip) { display: none; }
        .wl-look-summary { margin: 0; font-size: 12px; white-space: nowrap; }
        .wl-look-note { margin: 6px 0 0; font-size: 11.5px; color: #64748b; }
        .wl-tick-static { cursor: default; border-color: #e2e8f0; }
        .wl-tick-static:hover { border-color: #e2e8f0; }
        .wl-onlist, .wl-addone { flex: none; width: 30px; height: 30px; margin-right: 8px; box-sizing: border-box; padding: 0; display: flex; align-items: center; justify-content: center; border-radius: 999px; }
        .wl-onlist { background: #2563eb; color: #ffffff; }
        .wl-addone { border: 1px solid #bfdbfe; background: #eff6ff; color: #2563eb; cursor: pointer; }
        .wl-addone:hover { background: #dbeafe; }
        #wl-view.wl-looking .wl-look-bar {
            flex: none; display: grid; grid-template-columns: 1.5fr 1fr; gap: 10px;
            padding: 12px 16px calc(18px + env(safe-area-inset-bottom, 0px)); border-top: 1px solid #e2e8f0; background: #ffffff;
        }
        .wl-look-bar .wl-btn[hidden] { display: none; }
        .wl-look-bar .wl-count { margin-left: 2px; letter-spacing: 0; }
        .wl-look-bar .wl-btn[hidden] + .wl-btn { grid-column: 1 / -1; }

        /* ----- My Watchlist: its own page on laptops, in place of the filters and results ----- */
        @media (min-width: 1024px) {
            body.wl-page-open > main { display: none; }
            #wl-view { position: static; z-index: auto; }
            #wl-view .wl-backdrop, .wl-grabber, .wl-close, .wl-title-icon, .wl-row-delete { display: none; }
            .wl-panel {
                position: static; max-width: 1280px; margin: 0 auto; padding: 0 40px; box-sizing: border-box; display: block; overflow: visible;
                background: none; border-radius: 0; box-shadow: none; transform: none; transition: none;
            }
            .wl-head { padding: 0; border: 0; touch-action: auto; -webkit-user-select: auto; user-select: auto; }
            .wl-back {
                display: flex; align-items: center; gap: 4px; padding: 0; border: 0; background: none; color: #94a3b8; cursor: pointer;
                font: inherit; font-size: 10px; font-weight: 700; text-transform: uppercase; letter-spacing: 0.05em;
            }
            .wl-back:hover { color: #2563eb; }
            .wl-titlebar { flex-wrap: nowrap; align-items: baseline; gap: 16px; max-width: 860px; margin-top: 12px; padding-bottom: 14px; border-bottom: 1px solid #e2e8f0; }
            .wl-titlebar h2 { flex: none; font-size: 28px; }
            .wl-summary { flex-basis: auto; margin: 0; }
            .wl-hint { display: block; margin-left: auto; font-size: 10px; font-style: italic; color: #94a3b8; white-space: nowrap; }
            .wl-scroll { max-width: 860px; overflow: visible; padding: 0; }
            .wl-row { background: none; }
            .wl-row-main { gap: 16px; padding: 12px 0; background: none; touch-action: auto; }
            .wl-row.wl-lifted .wl-row-main { background: #ffffff; }
            .wl-info { gap: 16px; }
            .wl-info img, .wl-info .wl-noposter { width: 56px; height: 84px; border-radius: 8px; }
            .wl-name { font-size: 16px; }
            .wl-overview { display: -webkit-box; -webkit-line-clamp: 1; -webkit-box-orient: vertical; overflow: hidden; margin-top: 6px; font-size: 12px; line-height: 1.4; color: #64748b; }
            .wl-grip { width: 28px; }
            .wl-clear-row { text-align: left; padding: 18px 0 0; }
            .wl-clear { padding-left: 0; }
            .wl-credit { display: block; margin: 40px 0 0; font-size: 11px; color: #94a3b8; }
            .wl-credit a { text-decoration: underline; }
            #wl-view.wl-looking .wl-titlebar { flex-wrap: wrap; row-gap: 0; padding-right: 0; }
            #wl-view.wl-looking .wl-titlebar h2 { font-size: 24px; }
            #wl-view.wl-looking .wl-look-bar {
                position: sticky; bottom: 0; max-width: 420px; padding: 14px 0 20px; border: 0;
                background: linear-gradient(rgba(248,250,252,0), #f8fafc 30%);
            }
            .wl-onlist, .wl-addone { margin-right: 0; }
        }

        /* ----- Cards over the page: Clear list, and a saved list opening ----- */
        .wl-card {
            position: absolute; left: 12px; right: 12px; bottom: calc(24px + env(safe-area-inset-bottom, 0px)); max-width: 420px; margin: 0 auto;
            box-sizing: border-box; padding: 16px 16px 14px; background: #ffffff; border-radius: 16px; box-shadow: 0 16px 40px rgba(15,23,42,0.35);
            opacity: 0; transform: translateY(16px); transition: opacity 0.2s, transform 0.25s cubic-bezier(0.2, 0.8, 0.2, 1);
        }
        .wl-layer:has(.wl-card) .wl-backdrop { background: rgba(15,23,42,0.35); }
        .wl-layer.wl-shown .wl-card { opacity: 1; transform: none; }
        .wl-card-center { left: 24px; right: 24px; top: 50%; bottom: auto; max-width: 360px; padding: 20px 18px 16px; border-radius: 18px; text-align: center; transform: translateY(-45%); }
        .wl-layer.wl-shown .wl-card-center { transform: translateY(-50%); }
        .wl-card-title { margin: 0; font-size: 17px; font-weight: 800; color: #0f172a; }
        .wl-card-text { margin: 6px 0 16px; font-size: 12.5px; line-height: 1.45; color: #64748b; }
        .wl-card .wl-btn + .wl-btn { margin-top: 8px; }
        .wl-card .wl-btn-plain { margin-top: 4px; }
        .wl-card .wl-kicker { display: block; }
        .wl-card .wl-card-title { margin-top: 4px; }
        .wl-card .wl-made { display: flex; margin-top: 6px; font-size: 12.5px; }
        .wl-card > .wl-btn-blue { margin-top: 14px; }
        .wl-card-pair { display: grid; grid-template-columns: 1fr 1fr; gap: 10px; margin-top: 10px; }
        .wl-card-pair .wl-btn + .wl-btn { margin-top: 0; }
        .wl-strip { display: flex; align-items: center; gap: 6px; margin-top: 12px; }
        .wl-strip img, .wl-strip .wl-noposter { width: 38px; height: 57px; object-fit: cover; border-radius: 5px; box-shadow: 0 1px 3px rgba(15,23,42,0.25); }
        .wl-strip-more { margin-left: 2px; font-size: 12px; font-weight: 700; color: #94a3b8; }
        #wl-merge .wl-card-text { margin: 10px 0 0; font-size: 12px; }
        @media (min-width: 1024px) {
            .wl-card { top: 50%; bottom: auto; transform: translateY(-45%); }
            .wl-layer.wl-shown .wl-card { transform: translateY(-50%); }
        }

        @media (prefers-reduced-motion: reduce) {
            .wl-layer *, .wl-mark, .wl-mark svg, .wl-count, .wl-toast { transition: none !important; animation: none !important; }
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
        reloadList(); // another tab may have changed it while the page's data loaded
        if (!shownItems().length) resetFilters();
        shownCount = shownItems().length; // no bounce for the count the page opens with
        if (items.length && !listMade) { // a list saved before it kept a time: date it now, once
            listMade = new Date();
            saveItems();
        }
        buildFilterSection();
        buildButtons();
        buildToasts();
        buildView();
        buildPeek();
        buildConfirm();
        buildMergeCard();
        watchGrid();
        refreshChrome();
        addEventListener('hashchange', () => route());
        queueMicrotask(() => route(true)); // once this first updateResults is done
    }

    window.watchlist = {
        // Hooks for index.html
        filter, addPills, hasActiveFilters, clearFilters,
        // For the console and tests
        add, remove, toggle, setWatched, reorder, clear: clearList, peek: openPeek, open: showMyWatchlist, close: () => closeView(),
        items: () => items.map(item => ({ ...item })),
    };
})();
