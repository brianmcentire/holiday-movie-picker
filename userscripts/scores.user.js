// ==UserScript==
// @name         Season's Screenings: IMDb and Rotten Tomatoes scores
// @namespace    https://brianmcentire.github.io/holiday-movie-picker/
// @version      2.4
// @description  Adds IMDb, the Tomatometer and the Popcornmeter after the rating in the movie preview and on My Watchlist's rows, from MDBList.
// @match        https://brianmcentire.github.io/holiday-movie-picker/*
// @match        http://localhost:8000/*
// @grant        GM_xmlhttpRequest
// @grant        GM_getValue
// @grant        GM_setValue
// @grant        GM_registerMenuCommand
// @connect      api.mdblist.com
// @run-at       document-idle
// @updateURL    https://raw.githubusercontent.com/brianmcentire/holiday-movie-picker/main/userscripts/scores.user.js
// @downloadURL  https://raw.githubusercontent.com/brianmcentire/holiday-movie-picker/main/userscripts/scores.user.js
// ==/UserScript==

// Tampermonkey updates itself from GitHub, so raise @version with every change, or it won't notice.

// Where the scores come from:
// 1. scores.json, next to the site. The weekly data build fetches every film's scores from MDBList into it; the site
//    itself never loads it. It's read once per page load, so every preview after that shows its scores instantly.
//    Old scores are always shown; each score's tooltip says when it was fetched.
// 2. MDBList directly, only for a film the file doesn't have scores for (one added since the last build), or when
//    the file can't be read. That needs your MDBList key, which lives in Tampermonkey's storage, never in this file.
//    Set it by clicking "Scores: add your MDBList key" in a preview (shown when the file can't be read), or from the
//    Tampermonkey menu → "Set MDBList API key". Live scores are kept for 3 days per movie.
// A score shows only when it's a real number: never an icon or a label on its own. The IMDb score links to the film's
// IMDb page and the 🍅 and 🍿 scores to its Rotten Tomatoes page, when the film's IMDb id or RT address is known.

(() => {
    'use strict';

    const KEEP_FOR = 3 * 24 * 60 * 60 * 1000;
    const FILE_TIMEOUT = 8000;
    const pending = new Map(); // TMDB id → the live request in flight

    // ---------- Checking values: the file and MDBList's replies are never trusted ----------
    const asNumber = v => (typeof v === 'number' ? v : typeof v === 'string' && v.trim() !== '' ? Number(v) : NaN);
    const validImdb = v => { const n = asNumber(v); return n >= 1 && n <= 10 ? Math.round(n * 10) / 10 : null; };
    const validPercent = v => { const n = asNumber(v); return n >= 1 && n <= 100 ? Math.round(n) : null; }; // 0 means unknown
    const validImdbId = v => (typeof v === 'string' && /^tt\d+$/.test(v) ? v : null);
    // An RT address as "m/elf", "/m/elf" or "https://www.rottentomatoes.com/m/elf"
    function validRt(v) {
        if (typeof v !== 'string') return null;
        const path = v.trim().replace(/^https?:\/\/(www\.)?rottentomatoes\.com\//i, '').replace(/^\/+/, '').replace(/\/+$/, '');
        return /^m\/[a-z0-9_\-]+$/.test(path) ? path : null;
    }
    // "2026-11-16" → "Nov 16"
    function shortDate(v) {
        if (typeof v !== 'string' || !/^\d{4}-\d{2}-\d{2}$/.test(v)) return null;
        const d = new Date(`${v}T00:00:00Z`);
        return isNaN(d) ? null : d.toLocaleDateString('en-US', { month: 'short', day: 'numeric', timeZone: 'UTC' });
    }
    function scoresFrom(raw, when) {
        raw = raw && typeof raw === 'object' ? raw : {};
        return {
            imdb: validImdb(raw.imdb), tomatoes: validPercent(raw.tomatoes), popcorn: validPercent(raw.popcorn),
            imdbId: validImdbId(raw.imdbId ?? raw.imdb_id), rt: validRt(raw.rt), when,
        };
    }
    const hasScores = s => !!(s && (s.imdb || s.tomatoes || s.popcorn));

    // ---------- 1. scores.json ----------
    let fileLoad = null;
    function loadFile() {
        if (!fileLoad) {
            fileLoad = (async () => {
                const stop = new AbortController();
                const timer = setTimeout(() => stop.abort(), FILE_TIMEOUT);
                try {
                    const r = await fetch(new URL('scores.json', location.href), { cache: 'no-cache', signal: stop.signal });
                    if (!r.ok) throw new Error(`HTTP ${r.status}`);
                    const data = await r.json();
                    if (!data || data.v !== 1 || !data.movies || typeof data.movies !== 'object') throw new Error('not the expected format');
                    return data;
                } catch (e) {
                    console.warn('[scores] scores.json can’t be read, so scores come from MDBList directly:', e?.message || e);
                    return null;
                } finally {
                    clearTimeout(timer);
                }
            })();
        }
        return fileLoad;
    }

    function fromFile(file, id) {
        const entry = Object.prototype.hasOwnProperty.call(file.movies, id) ? file.movies[id] : null;
        if (!entry) return null;
        const date = shortDate(entry.at) || shortDate(file.updated);
        return scoresFrom(entry, date ? `updated ${date}` : 'from the weekly build');
    }

    // ---------- 2. MDBList directly ----------
    // From the Tampermonkey menu, or by clicking the note in the preview when there's no key or a wrong one
    function askForKey() {
        const key = prompt('Your MDBList API key (mdblist.com → Preferences → API):', GM_getValue('mdblistKey', ''));
        if (key === null) return;
        GM_setValue('mdblistKey', key.trim());
        GM_setValue('liveScores', {}); // a new key gets fresh scores
        document.querySelector('#wl-peek .ss-scores')?.remove();
        fill();
    }
    GM_registerMenuCommand('Set MDBList API key', askForKey);

    // MDBList's reply: { imdb_id: 'tt0319343', ratings: [{ source: 'imdb', value: 7.1 }, { source: 'tomatoes', value: 85,
    // url: … }, { source: 'popcorn', value: 79 }, …] }. The audience score was called tomatoesaudience on its older host.
    function pick(data) {
        const ratings = Array.isArray(data?.ratings) ? data.ratings : [];
        const item = (...sources) => ratings.find(r => r && sources.includes(r.source) && asNumber(r.value) > 0);
        const tomatoes = item('tomatoes'), popcorn = item('popcorn', 'tomatoesaudience');
        return {
            imdb: item('imdb')?.value, tomatoes: tomatoes?.value, popcorn: popcorn?.value,
            imdbId: data?.imdb_id ?? data?.ids?.imdb, rt: validRt(tomatoes?.url) || validRt(popcorn?.url),
        };
    }

    function live(id) {
        const key = GM_getValue('mdblistKey', '');
        const saved = GM_getValue('liveScores', {})[id];
        if (saved && Date.now() - saved.at < KEEP_FOR) return Promise.resolve(scoresFrom(saved, 'live'));
        if (!pending.has(id)) {
            pending.set(id, new Promise(resolve => {
                const fail = why => { pending.delete(id); console.warn('[scores]', id, why); resolve(null); };
                GM_xmlhttpRequest({
                    method: 'GET',
                    url: `https://api.mdblist.com/tmdb/movie/${id}?apikey=${encodeURIComponent(key)}`,
                    timeout: 10000,
                    onload: r => {
                        pending.delete(id);
                        if (r.status === 401 || r.status === 403) return resolve({ problem: 'Scores: MDBList didn’t accept your key. Change it' });
                        if (r.status !== 200) return fail(`HTTP ${r.status}`);
                        let data;
                        try { data = JSON.parse(r.responseText); } catch (e) { return fail('not JSON'); }
                        const picked = { ...pick(data), at: Date.now() };
                        const all = GM_getValue('liveScores', {});
                        all[id] = picked;
                        GM_setValue('liveScores', all);
                        resolve(scoresFrom(picked, 'live'));
                    },
                    onerror: () => fail('network error'),
                    ontimeout: () => fail('timed out'),
                });
            }));
        }
        return pending.get(id);
    }

    // The file first; MDBList only for what the file can't answer
    async function scoresFor(id) {
        const file = await loadFile();
        const hasKey = !!GM_getValue('mdblistKey', '');
        if (file) {
            const s = fromFile(file, id);
            if (hasScores(s)) return s; // even with a gap: the file came from MDBList, so asking again wouldn't fill it
            if (!hasKey) return null;
            const fresh = await live(id);
            // Keep the file's links when the live reply has none
            return fresh && !fresh.problem && s ? { ...fresh, imdbId: fresh.imdbId || s.imdbId, rt: fresh.rt || s.rt } : fresh;
        }
        return hasKey ? live(id) : { problem: 'Scores: add your MDBList key' };
    }

    // ---------- Showing them ----------
    // Each score is styled like the site's own rating; the emoji stay upright beside the italic numbers
    const style = document.createElement('style');
    style.textContent = `
        .ss-scores a.ss-link { text-decoration: none; cursor: pointer; } /* the grey comes from wl-rating */
        .ss-scores a.ss-link:hover, .ss-scores a.ss-link:focus-visible { text-decoration: underline; }`;
    document.head.append(style);

    function part(emoji, text, href, title) {
        const el = document.createElement(href ? 'a' : 'span');
        el.className = href ? 'wl-rating ss-link' : 'wl-rating';
        el.style.cssText = 'display: inline-flex; align-items: center; gap: 3px;';
        if (href) {
            el.href = href;
            el.target = '_blank';
            el.rel = 'noopener noreferrer';
            el.draggable = false;
            el.addEventListener('click', e => e.stopPropagation()); // opens the new tab; the preview stays as it is
        }
        if (title) el.title = title;
        if (emoji) {
            const e = document.createElement('span');
            e.style.cssText = 'font-style: normal; font-size: 1.1em; line-height: 1;';
            e.textContent = emoji;
            el.append(e);
        }
        el.append(text);
        return el;
    }

    function show(box, s) {
        if (s?.problem) {
            const note = part('', s.problem);
            note.setAttribute('role', 'button');
            note.tabIndex = 0;
            note.style.cssText += 'cursor: pointer; text-decoration: underline; color: #2563eb;';
            note.addEventListener('click', e => { e.preventDefault(); e.stopPropagation(); askForKey(); });
            note.addEventListener('keydown', e => { if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); askForKey(); } });
            box.append(note);
        } else if (hasScores(s)) {
            const by = s.when ? `MDBList, ${s.when}` : 'MDBList';
            const imdbPage = s.imdbId && `https://www.imdb.com/title/${s.imdbId}/`;
            const rtPage = s.rt && `https://www.rottentomatoes.com/${s.rt}`;
            if (s.imdb) box.append(part('', `IMDb ${s.imdb.toFixed(1)}`, imdbPage, `IMDb rating · ${by}`));
            if (s.tomatoes) box.append(part('🍅', `${s.tomatoes}%`, rtPage, `Tomatometer · ${by}`));
            if (s.popcorn) box.append(part('🍿', `${s.popcorn}%`, rtPage, `Popcornmeter · ${by}`));
        }
        if (box.childElementCount) { // nothing to show takes no space at all
            box.style.display = 'inline-flex';
            fit(box);
        }
    }

    // Spacing between the scores (the TMDB rating included): roomy on the laptop panel, 12px on a phone when the
    // whole line fits, else the site's own 8px. When even 8px doesn't fit (narrow phones), the scores move together
    // onto their own line under the rating, rather than running off the edge. A score is never split.
    const PANEL = matchMedia('(min-width: 720px) and (min-height: 560px)'); // the site's large centered preview
    const SITE_GAP = 8; // .wl-meta's own gap, between the rating and this box
    function fit(box) {
        const meta = box.parentElement;
        const column = meta?.parentElement;
        if (!meta || !column) return;
        const cs = getComputedStyle(column);
        const edge = () => column.getBoundingClientRect().right - parseFloat(cs.paddingRight) + 0.5;
        const gaps = PANEL.matches ? [14, 12, SITE_GAP] : [12, SITE_GAP];
        const tryGaps = ownLine => gaps.find(g => {
            box.style.gap = `${g}px`;
            box.style.marginLeft = ownLine ? '0' : `${g - SITE_GAP}px`;
            return box.getBoundingClientRect().right <= edge();
        });
        meta.style.flexWrap = '';
        if (tryGaps(false) !== undefined) return;
        meta.style.flexWrap = 'wrap';
        meta.style.rowGap = '4px';
        if (tryGaps(true) === undefined) { box.style.gap = `${SITE_GAP}px`; box.style.marginLeft = '0'; }
    }
    // Turning a phone or resizing a window changes the room
    addEventListener('resize', () => {
        const box = document.querySelector('#wl-peek .ss-scores');
        if (box?.childElementCount) fit(box);
    }, { passive: true });

    // The preview redraws its content each time it opens; add the scores after the rating whenever they're missing.
    // The box is there, hidden, from the start: it marks the film as handled while its scores load.
    function fill() {
        const meta = document.querySelector('#wl-peek .wl-meta');
        // The TMDB id: from the rating, which links to TMDB, or from the older More on TMDB button
        const tmdbLink = document.querySelector('#wl-peek .wl-meta a.wl-rating-link, #wl-peek a[data-peek="tmdb"]');
        const id = tmdbLink?.href.match(/\/movie\/(\d+)/)?.[1];
        if (!meta || !id || meta.querySelector(`.ss-scores[data-id="${id}"]`)) return;
        meta.querySelector('.ss-scores')?.remove();
        const box = document.createElement('span');
        box.className = 'ss-scores';
        box.dataset.id = id;
        box.style.cssText = 'display: none; align-items: center; gap: 8px; white-space: nowrap;';
        meta.append(box);
        scoresFor(id).then(s => { if (box.isConnected) show(box, s); });
    }

    // My Watchlist's rows (2.4): the same scores after each row's rating, from the file only, so a long list never
    // sends a burst of requests to MDBList (the preview still asks it for a film the file lacks). Plain text here:
    // the row itself is the link that opens the preview, and that has the linked scores. On a narrow phone row they
    // wrap under the rating.
    const ROW_GAP = matchMedia('(min-width: 1024px)'); // the laptop page; the phone and tablet sheet is narrower
    function fillRows() {
        const rows = document.querySelectorAll('#wl-view .wl-row[data-id]');
        if (!rows.length) return;
        loadFile().then(file => {
            if (!file) return;
            for (const row of rows) {
                const meta = row.querySelector('.wl-meta');
                const id = row.dataset.id;
                if (!meta || meta.querySelector(`.ss-scores[data-id="${id}"]`)) continue;
                const s = fromFile(file, id);
                if (!hasScores(s)) continue;
                const box = document.createElement('span');
                box.className = 'ss-scores';
                box.dataset.id = id;
                const gap = ROW_GAP.matches ? 12 : 8;
                box.style.cssText = `display: inline-flex; align-items: center; gap: ${gap}px; margin-left: ${gap - SITE_GAP}px; white-space: nowrap;`;
                const by = s.when ? `MDBList, ${s.when}` : 'MDBList';
                if (s.imdb) box.append(part('', `IMDb ${s.imdb.toFixed(1)}`, null, `IMDb rating · ${by}`));
                if (s.tomatoes) box.append(part('🍅', `${s.tomatoes}%`, null, `Tomatometer · ${by}`));
                if (s.popcorn) box.append(part('🍿', `${s.popcorn}%`, null, `Popcornmeter · ${by}`));
                meta.style.flexWrap = 'wrap';
                meta.style.rowGap = '2px';
                meta.append(box);
            }
        });
    }

    loadFile(); // fetched as the page loads, so even the first preview is instant
    new MutationObserver(() => { fill(); fillRows(); }).observe(document.body, { childList: true, subtree: true });
    fill();
    fillRows();
})();
