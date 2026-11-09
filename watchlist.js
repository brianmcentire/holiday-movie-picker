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
    const yearOf = m => (m.date && m.date !== '0000-00-00' ? m.date.split('-')[0] : 'TBA');
    const ratingOf = m => (m.votes ? `${m.rating.toFixed(1)}/10` : 'Not yet rated');
    const tmdbURL = id => `https://www.themoviedb.org/movie/${id}`;
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
    };

    function icon(name, size, { fill = 'none', stroke = 2, cls = '', label = '' } = {}) {
        const a11y = label ? `role="img" aria-label="${label}"` : 'aria-hidden="true"';
        return `<svg ${a11y} class="${cls}" width="${size}" height="${size}" viewBox="0 0 24 24" fill="${fill}" stroke="currentColor" stroke-width="${stroke}" stroke-linecap="round" stroke-linejoin="round">${ICONS[name]}</svg>`;
    }

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
        else markCards();
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
            if (el.tagName !== 'SCRIPT' && el.id !== 'wl-toasts') el.inert = !!top && el !== top.el;
        }
    }

    addEventListener('keydown', e => {
        if (e.key !== 'Escape' || !openLayers.length) return;
        e.preventDefault();
        openLayers[openLayers.length - 1].dismiss();
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
                ${m.poster ? `<img src="${esc(m.poster)}" alt="" draggable="false">` : '<span class="wl-noposter"></span>'}
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

        @media (prefers-reduced-motion: reduce) {
            .wl-layer *, .wl-mark, .wl-mark svg { transition: none !important; animation: none !important; }
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
        buildPeek();
        watchGrid();
        refreshChrome();
    }

    window.watchlist = {
        // Hooks for index.html
        filter, addPills, hasActiveFilters, clearFilters,
        // For the console and tests
        add, remove, toggle, setWatched, reorder, peek: openPeek,
        items: () => items.map(item => ({ ...item })),
    };
})();
