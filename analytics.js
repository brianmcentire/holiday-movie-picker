// Usage stats through GoatCounter (goatcounter.com): page views, plus two events that show
// which lists people pass around:
//   share-clicked?<list>       someone pressed Share results
//   opened-shared-link?<list>  a visit arrived on a link with filters (almost always a shared link)
// <list> is the page's own query string (with, without, q, sort...), so it opens the same view.
//
// Self-contained: index.html loads this with one <script> tag and nothing else refers to it.
// To remove it, delete that tag and this file.
//
// Off until GOATCOUNTER_CODE is set; on localhost it never sends, it logs to the console instead.
(() => {
    const GOATCOUNTER_CODE = 'seasons-screenings'; // seasons-screenings.goatcounter.com

    const live = GOATCOUNTER_CODE !== '' && !['localhost', '127.0.0.1'].includes(location.hostname);

    // Only the app's own parameters (drops fbclid, utm_* and the like), sorted so the same
    // list always reads the same
    const LIST_KEYS = ['with', 'without', 'ratings', 'levels', 'q', 'sort', 'dir'];
    function listFrom(search) {
        const kept = [...new URLSearchParams(search)]
            .filter(([key]) => LIST_KEYS.includes(key))
            .sort(([k1, v1], [k2, v2]) => k1.localeCompare(k2) || v1.localeCompare(v2));
        return new URLSearchParams(kept).toString();
    }

    // Events wait here until count.js has loaded
    const pending = [];
    function record(name, title, list) {
        const event = { path: list ? `${name}?${list}` : name, title, event: true };
        if (!live) return console.info('[analytics]', event.path);
        if (window.goatcounter?.count) window.goatcounter.count(event);
        else pending.push(event);
    }

    // Read the address now, before the page tidies it to match its own state
    const arrivedWith = listFrom(location.search);
    const navType = performance.getEntriesByType?.('navigation')?.[0]?.type;
    if (arrivedWith && (navType === undefined || navType === 'navigate')) {
        record('opened-shared-link', 'Opened a shared link', arrivedWith); // not reloads or Back
    }

    // By the time Share is pressed, the address matches what's on screen
    document.addEventListener('click', e => {
        if (e.target.closest?.('#share-btn')) record('share-clicked', 'Share clicked', listFrom(location.search));
    });

    if (!live) return;
    // Page views without the query, so every filter combination doesn't get its own row
    window.goatcounter = { path: p => p.split('?')[0] };
    const script = document.createElement('script');
    script.async = true;
    script.src = 'https://gc.zgo.at/count.js';
    script.dataset.goatcounter = `https://${GOATCOUNTER_CODE}.goatcounter.com/count`;
    script.onload = () => pending.splice(0).forEach(event => window.goatcounter.count(event));
    document.head.appendChild(script);
})();
