// ==UserScript==
// @name         Season's Screenings: IMDb and Rotten Tomatoes scores
// @namespace    https://brianmcentire.github.io/holiday-movie-picker/
// @version      1.0
// @description  Adds IMDb, the Tomatometer and the Popcornmeter after the rating in the movie preview, from MDBList.
// @match        https://brianmcentire.github.io/holiday-movie-picker/*
// @match        http://localhost:8000/*
// @grant        GM_xmlhttpRequest
// @grant        GM_getValue
// @grant        GM_setValue
// @grant        GM_registerMenuCommand
// @connect      api.mdblist.com
// @run-at       document-idle
// ==/UserScript==

// Your MDBList key lives in Tampermonkey's storage, never in this file: Tampermonkey menu → "Set MDBList API key".
// Scores are kept for 3 days per movie (MDBList refreshes them every few days), so reopening a preview costs nothing.

(() => {
    'use strict';

    const KEEP_FOR = 3 * 24 * 60 * 60 * 1000;
    const pending = new Map(); // TMDB id → the request in flight

    GM_registerMenuCommand('Set MDBList API key', () => {
        const key = prompt('Your MDBList API key (mdblist.com → Preferences → API):', GM_getValue('mdblistKey', ''));
        if (key === null) return;
        GM_setValue('mdblistKey', key.trim());
        GM_setValue('scores', {}); // a new key gets fresh scores
        document.querySelector('#wl-peek .ss-scores')?.remove();
        fill();
    });

    // MDBList's ratings: [{ source: 'imdb', value: 7.1 }, { source: 'tomatoes', value: 85 }, { source: 'popcorn', value: 79 }, ...]
    // The audience score was called tomatoesaudience on MDBList's older host.
    function pick(ratings) {
        const find = (...sources) => ratings.find(r => sources.includes(r.source) && Number(r.value) > 0)?.value;
        return { imdb: find('imdb'), tomatoes: find('tomatoes'), popcorn: find('popcorn', 'tomatoesaudience') };
    }

    function scoresFor(id) {
        const key = GM_getValue('mdblistKey', '');
        if (!key) return Promise.resolve({ problem: 'Scores: set your MDBList key in the Tampermonkey menu' });
        const saved = GM_getValue('scores', {})[id];
        if (saved && Date.now() - saved.at < KEEP_FOR) return Promise.resolve(saved);
        if (!pending.has(id)) {
            pending.set(id, new Promise(resolve => {
                const fail = why => { pending.delete(id); console.warn('[scores]', id, why); resolve({}); };
                GM_xmlhttpRequest({
                    method: 'GET',
                    url: `https://api.mdblist.com/tmdb/movie/${id}?apikey=${encodeURIComponent(key)}`,
                    timeout: 10000,
                    onload: r => {
                        pending.delete(id);
                        if (r.status === 401 || r.status === 403) return resolve({ problem: 'Scores: MDBList didn’t accept your key' });
                        if (r.status !== 200) return fail(`HTTP ${r.status}`);
                        let data;
                        try { data = JSON.parse(r.responseText); } catch (e) { return fail('not JSON'); }
                        const scores = { ...pick(data.ratings || []), at: Date.now() };
                        const all = GM_getValue('scores', {});
                        all[id] = scores;
                        GM_setValue('scores', all);
                        resolve(scores);
                    },
                    onerror: () => fail('network error'),
                    ontimeout: () => fail('timed out'),
                });
            }));
        }
        return pending.get(id);
    }

    // Each score is styled like the site's own rating; the emoji stay upright beside the italic numbers
    function show(box, s) {
        const part = (emoji, text) => {
            const span = document.createElement('span');
            span.className = 'wl-rating';
            span.style.cssText = 'display: inline-flex; align-items: center; gap: 3px;';
            if (emoji) {
                const e = document.createElement('span');
                e.style.cssText = 'font-style: normal; font-size: 1.1em; line-height: 1;';
                e.textContent = emoji;
                span.append(e);
            }
            span.append(text);
            return span;
        };
        if (s.problem) return box.append(part('', s.problem));
        if (s.imdb) box.append(part('', `IMDb ${Number(s.imdb).toFixed(1)}`));
        if (s.tomatoes) box.append(part('🍅', `${Math.round(s.tomatoes)}%`));
        if (s.popcorn) box.append(part('🍿', `${Math.round(s.popcorn)}%`));
    }

    // The preview redraws its content each time it opens; add the scores after the rating whenever they're missing
    function fill() {
        const meta = document.querySelector('#wl-peek .wl-meta');
        const id = document.querySelector('#wl-peek a[data-peek="tmdb"]')?.href.match(/\/movie\/(\d+)/)?.[1];
        if (!meta || !id || meta.querySelector(`.ss-scores[data-id="${id}"]`)) return;
        meta.querySelector('.ss-scores')?.remove();
        const box = document.createElement('span');
        box.className = 'ss-scores';
        box.dataset.id = id;
        box.style.cssText = 'display: inline-flex; align-items: center; gap: 8px; white-space: nowrap;';
        meta.append(box);
        scoresFor(id).then(s => { if (box.isConnected) show(box, s); });
    }

    new MutationObserver(fill).observe(document.body, { childList: true, subtree: true });
    fill();
})();
