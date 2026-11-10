# Design decisions

What Season's Screenings settled on, and why where it isn't obvious. Mocks for most of this are on the design canvas (private to the owner). When something here changes, change this file with it.

## Ground rules

- **Shared links keep working forever.** People save and pass links around, so once a link format ships, every later version must open it the same way.
  - Only add params or values; never rename or remove one.
  - Ignore anything unrecognized instead of failing.
  - Put a version number in any new encoded format from day one.
  - Today that covers the query params `with`, `without`, `ratings`, `levels`, `q`, `sort` and `dir`. It also covers the watch list's `#watchlist` links below.
  - Filter buttons appear in links by their hidden names ("Holiday story"), not their labels, so labels can change freely.
  - Tags in links match ignoring case and spacing. A link to a tag that has since lost its sidebar button (a weekly data refresh can leave it with one film) still filters by it.
  - **The group defaults are part of the link format.** Links leave out Content Ratings and Holiday Intensity when they're at their defaults (every rating but R; the story and the setting), so changing a default, or adding a level, would quietly change what old links show. Don't, unless old links are taught the old defaults.
- **New features come in as modules that can come back out.** Usage stats live in `analytics.js`, and the watch list lives in `watchlist.js`. Each loads with one script tag and touches `index.html` only through a few lines marked `watch list hook`. Removing a feature means deleting its file, its tag and those lines.
- **Fun and light.** No accounts, no servers, no build step.
- **Libraries are pinned:** Tailwind 3.4.17 and lucide 1.70.0. Loaded as "latest", a new release could change or drop icons with no change here.

## Updates and caching

- GitHub Pages marks every file fresh for 10 minutes, then browsers check whether it changed (cheap when it hasn't). Visitors get a release within about 10 minutes, with no cache-busting needed. Open tabs and the Home Screen app pick it up on their next reload or launch, and reloading loses nothing: the list is stored on the device and the filters are in the address.
- The page and each module are cached separately. When a release changes `index.html` and `watchlist.js` in ways that must match, add a version to the script's address (`watchlist.js?v=2`) in the same commit.
- No service worker: an offline cache layer would make stale versions more likely, not less.

## Words

- **Holiday Intensity**, not Holiday Level. Level felt impersonal. The buttons read as answers under it ("It's the story", "It's the setting", "It's adjacent"). The summary chip says "Any holiday intensity". The hidden names in links stay "Holiday story" and so on.
  - Its buttons switch on and off only, never red. They're answers to pick, not tags to exclude. Older links that excluded a level open with the remaining levels picked, which shows the same movies.
  - **Content Ratings work the same way** (on and off, never red). With other ratings picked, an excluded rating was hidden anyway, so the red step changed nothing on screen but the chip. Older links that excluded a rating (`without=R`) open the same way as levels, with the same movies. *Changed during the review on Nov 9 without the owner; revert that one commit to bring the red step back.*
- **Clear filters** and **Reset filters**, not Clear all and Reset, so nobody worries a button might delete their watch list.
- **My Watchlist** where there's room: the laptop button, page and sheet titles, the filters section, tooltips. **My List** where it's tight: the phone's floating button and the "On My List" pill.
- **Made**, not Saved, for when a saved copy was created.
- **Friendly, never a warning.** No "keep it safe" or "don't lose it": it sounds scary and promises what a website can't guarantee.

## Search and results

- **Search ignores case, accents, apostrophes, full stops and extra spaces.** iPhone types it’s with a curly apostrophe and adds a space after a tapped suggestion; neither should hide movies. "cafe" finds Café, "its a wonderful" finds It's a Wonderful Life, "dr seuss" finds Dr. Seuss. The link keeps the search exactly as typed.
- **Search matches the start of a word.** "elf" finds Elf, Elf's and elfin, but not herself, selfish or twelfth; "ice" no longer finds police or nice. A word typed halfway still matches, so results keep changing with every letter ("gri" finds Grinch). Searching the middle of a word ("mas" for Christmas) doesn't. Why: "elf" found 116 films, mostly herself and himself, with the movie Elf 77th.
- **Well-known films with the search in their title come first** (`TITLE_BOOST` in index.html), so "elf" puts Elf first. "Well-known" is 1,000+ TMDB votes, about 80 of the 1,136 films in Nov 2026. Everything else keeps the chosen sort.
  - Why not every title match: "santa" put 55 Santa titles (Santa Bootcamp first) ahead of Klaus, and "christmas" put It's a Wonderful Life 657th. With the cutoff, Klaus is 4th, after The Santa Clause 1–3, and It's a Wonderful Life is 18th, after famous Christmas titles.
  - Only under Top Rated, User Rating and Total Votes, in their usual direction. Release Date, Alphabetical and Random stay strictly in order.
  - Tune it with `minVotes` (lower lets more titles through), or turn it off with `enabled: false`.
- **Nothing found, but hidden:** when the only matches are hidden by Content Ratings or Holiday Intensity (searching "die hard" with R and It's adjacent off), the empty results say "2 movies match, hidden by Content Ratings and Holiday Intensity." with **Show them**, which turns on every rating (or intensity) and keeps the search.
- **Random** is a fair shuffle, made again on every filter or search change, but not when only the watch list changes (ticking a movie off doesn't reshuffle the page).

## Phones and tablets

- **Folding the filters.** A **‹** at the top right of the filters folds them into a slim strip. The strip shows **›**, the word Filters and how many filters are on; tap anywhere on it to reopen. The device remembers the choice.
  - Columns with filters open → folded: upright phone 2 → 3; tablet 3 → 4; sideways phone 4 → 5.
- **Long tags wrap** in the narrow filters column instead of making it scroll sideways.
- **Sideways phones** (landscape, no taller than 500px):
  - The whole page scrolls, so Safari tucks its own bars away.
  - The filters stay pinned beside the results and scroll on their own. A swipe that starts on the filters never moves the page. While the top bar shows, the filters sit just below it, so it never covers their top.
  - The top bar is one row (title, search, sort). It slides away while scrolling down and comes straight back on any scroll up.
  - Why: locked to the screen with two scrolling halves, the page never scrolled, so Safari's bars never collapsed and little room was left.
  - Upright phones keep the two scrolling halves. Turning a phone upright again puts the page back at the top; otherwise a page scrolled sideways would hide the top bar with no way back.
- **Smaller posters in the grid:** phones and tablets load TMDB's 342-pixel posters instead of the 500-pixel ones, which is plenty for those card sizes. Each is about 60% smaller, with under half the pixels to decode while scrolling, and scrolling on an iPhone went from laggy to smooth. The one-per-row view and laptops keep the 500-pixel posters.
- **Laptops:** the filters' scrollbar appears only while the pointer is over them. It sat between filters and results looking like a stray mark. The results scroll with the page (wheel or the window's scrollbar).

## Usage stats

- GoatCounter, in `analytics.js`. It records page views plus two events:
  - `share-clicked?<list>`;
  - `opened-shared-link?<list>`, skipped on reloads and Back.

  `<list>` is the page's own query string.
- It never sends from this computer or the home network (private addresses and `.local` names), so testing on a phone over Wi-Fi doesn't count.

## Watch list

### What's stored

- Only TMDB movie IDs, in the user's order, with a watched flag. It's versioned and kept in localStorage, and everything else comes from the page's own data.
- A tab still running an older version never writes over a list saved by a newer one (a higher `v`), and keeps any fields it doesn't know about.
- IDs missing from the data stay stored but hidden; nothing breaks on them.
- The first add asks the browser to keep the storage (`navigator.storage.persist()`).

### On the results

- **Marks:**
  - On the list: a blue circle with a solid white bookmark at the poster's top right.
  - Watched: the poster and card fade, with a diagonal green WATCHED ribbon across the top-left corner.
  - Not on the list: nothing.
- **Icon: the bookmark.** The first idea, popcorn, read as a trash can. A bookmark means "save for later" everywhere.
- **Adding on phones:** tap a poster for a **peek**. It's a sheet with the poster, the basics and the overview, plus:
  - **Add to My Watchlist** (or, when the movie is on the list, **Watched it** and **Remove**);
  - **More on TMDB**.

  Why: an empty bookmark on every tile was clutter. Tapping a poster is what everyone does anyway, and phone tiles hide the overview, so the peek gives something back. The cost is one extra tap to reach TMDB.
- **Adding on laptops:** the bookmark appears only on the poster under the pointer. Clicking it adds the movie; clicking anywhere else on the card still opens TMDB.

### My List button

- It shows once the list has a movie and disappears when the list is empty. The count bounces when it changes.
- Phones: it floats at the bottom right ("My List" and the count). Why: a whole row at the top cost too much room on phones.
- Laptops: "My Watchlist" with the count, in the header row next to search.

### My Watchlist view

- Laptops: its own page, with "‹ All movies" to go back. Phones: a sheet over everything. The link `#watchlist` opens it.
- The summary line is "3 of 9 watched · Hide watched · Save a copy".
- Each row has a circle to mark it watched. Rows reorder by the drag grip.
- Removing: on phones, swipe a row left, with Undo; on laptops, × on hover.
- On the laptop page, the top bar's search narrows the list. The rule is that the top bar acts on whatever is below it.
- **Clear list:** a quiet button at the bottom. It asks first and offers **Save a copy first**. Once confirmed, My Watchlist closes and you're back on the main screen, not looking at an empty list. Undo shows there and brings the list back without reopening it.
- There's no "saved on" date in the list, and no "changes since your last save" nudge. The site can't know whether a copy was actually kept, since Save a copy only opens the share sheet or copies a link.

### Filters

- A **My Watchlist** section sits just under Holiday Intensity. It shows only when the list has movies.
- It holds two pills that work like every other filter pill (once: only these; twice: hide these; third: back to all):
  - **On My List**;
  - **Watched**, where tapping twice hides what you've seen.
- They show as chips above the results. Clear filters and Reset filters reset them. They're remembered on the device.
- They never go into shared links: they're about your own list, and a friend's is different.
- Why: this replaced an Unwatched | Watched | All switch under the search box, which cluttered the top and cost a row on phones.

### Saved copies and sharing a list

- **Save a copy** opens the share sheet on phones and copies the link on laptops. When it copies, it says "Link copied" and "Send it to whoever you're watching with, or to yourself for later. It opens this exact list."
- The link is `#watchlist=1&made=2026-11-09T19:41Z&m=771,10719w,…`:
  - `watchlist=1` is the format version;
  - `made` is when the copy was made, in ISO 8601 universal time to the minute;
  - `m` is the movies in order, with `w` marking watched.

  Unknown keys are ignored, and a missing or garbled `made` just shows no date.
- **Times** show in each phone's own time zone, and nobody is asked for one: "Made Sun, Nov 9 at 2:41 PM · 2 days ago".
- **Share wording:**
  - title: "My Watchlist · Nov 9, 2:41 PM";
  - message: "My watchlist from Season's Screenings: 9 movies, made Sun, Nov 9 at 2:41 PM".

  iPhone's share sheet shows only the title and the site's name, never the link, and cuts titles off at about 30 characters.
- **Opening a saved link:**
  - **Your list is empty:** it's restored as it was, watched marks included. It's your own backup, or a partner handing over the whole list.
  - **You already have a list:** a card, "Shared with you", shows the count, the Made line, a poster strip and how many are new to your list. The buttons are **Add 7 new**, **Just look** and **No thanks**.
- **Just look** shows the shared list as it was sent, with its watched marks, without changing yours.
  - A **Hide watched** link sits beside "9 movies, 3 watched", the same as on your own list.
  - Movies already on your list show the blue bookmark. Tapping another's bookmark adds it right away, with no save step.
  - **Add the rest**, with a count, adds every showing movie not yet on your list. The count drops as you bookmark, and the button goes away at 0, leaving **Done**.
- **Watched marks when bringing a list in:** adding never changes your own marks, and movies added to an existing list come in unwatched. Why: couples share lists. A restore carries the marks; adding means "I want to watch these too".

### The list in the page's address

- On every browser, the page's address carries a copy of the list, kept up to date as it changes: `#watchlist=1&backup=1&made=…&m=…`, plus `&open=1` while My Watchlist is open. On iPhone, Safari shows only the site's name, so nobody sees it there.
- **Why:** bookmarks, history and private windows bring the list back with it, and so does reopening a tab after Safari has cleared the site's storage. On iPhone, Add to Home Screen only keeps the copy if the page was loaded with it; see "Add to Home Screen".
- **Opening an address with `backup=1`:**
  - Your list is empty: it fills from the copy ("Restored your watchlist", with Undo). Undo takes out only the movies the copy brought in, so anything added since stays.
  - You already have a list: the copy is ignored.
  - **After Clear list, an older copy still restores.** An old bookmark, a reopened tab or a history entry brings the cleared list back onto the empty list, with Undo. Why: simpler, and the owner wants it that way (Nov 10). An earlier build ignored copies made before the clear; that's gone.
  - It's read only when the page opens with that address, not when the address changes in an open tab, so going Back can't undo a Clear list.

  A friend who's sent an address-bar link and has no list starts with the sender's list, with Undo. That's intended.
- Share results never carries it.

### Add to Home Screen (tip off for now)

- **The Home Screen tip is built but switched off** (`HOME_SCREEN_TIP` in `watchlist.js`). Two findings on iOS 27 made it a poor promise for now:
  - **iPhone saves the address the page was loaded with,** not the one the page updates afterwards. A list built after the page loaded isn't in that address, so the Home Screen app starts empty. A page reloaded with the list in its address does carry it (tested on the simulator).
  - **A Home Screen app keeps its own storage, separate from Safari's,** and links someone texts you always open in Safari. People who use both would have two lists drifting apart.
- **When it's back, the tip will read:** "Added Klaus." then, on its own line, "Add Season's Screenings to your Home Screen: open Safari's Share menu ⬆ and choose Add to Home Screen."
  - It names Safari's Share menu, with Safari's icon, because Add to Home Screen exists only there. Our Share results button can't do it.
  - The planned places are once right after the very first add, and at the bottom of the list on phones. It's iPhone Safari only, and never shown once closed or once the site is on the Home Screen.
- **The app name is "Season's Screenings"** (the manifest's `short_name`), and the manifest has no fixed start address.
- **A Home Screen app's launch address only fills an empty list.** It always opens at the address it was added with, so any list in that address (the backup copy or a shared copy) is a snapshot from then. It's never shown as "Shared with you", and it never reopens My Watchlist at launch.
- **Why it would matter:** opened from the Home Screen, the site runs full screen without Safari's bars, and Safari doesn't clear its storage after 7 days without a visit. The list in the address already helps with the 7-day clearing in Safari, since reopening the tab or a history entry brings it back.

### Toasts

- Notes that are sentences or instructions stay on screen twice as long as the short "Removed X · Undo" ones, and have an × to close them early.

## Decided against

- **Laptop list:** a side drawer and a centered panel. My Watchlist is its own page.
- **Look of the marks:** checkmarks on the results; a watched sash alone; fading alone. The popcorn icon, plus heart, star, ticket, clock and plain words.
- **Ways to add:** an empty bookmark on every tile; long-press to add (hidden, and it fights iPhone's own preview); double-tap to add (hidden, and it slows every tap); an "add mode" switch.
- **Top of the page:** an Unwatched | Watched | All switch, and a My List row under the search box.
- **Names for Holiday Level:** Holiday Spirit (reads as mood: a gloomy film set at Christmas still counts as "the setting"), Tinsel Factor (Christmas-only, and some films are Hanukkah, New Year's or Thanksgiving), and the question-style names.

## Revisit later

- **Films without a holiday level** (when the classifier couldn't run) are hidden by the default Holiday Intensity, though the footer mentions them. They show with Any holiday intensity. Fine as a safe default, or let them through.
- **TV-G, TV-PG and TV-Y7:** the builder allows them but Content Ratings has no buttons for them, so such a film would be hidden by default and couldn't be picked. None in today's data.
- **Pill text contrast:** white 10–11px text on the blue, green and red pills is about 3.3–3.8:1, under the 4.5:1 guideline for small text.
- **The Undo after tapping a bookmark** may be redundant, since tapping the bookmark again removes the movie. Kept for now as feedback.
- **"Mark them watched too"** in Just look, for couples who watch together while only one of them marks things. Not built.
- **Mail's subject line:** check on a real iPhone whether Mail uses the share title as the subject.
- **Bringing back the Home Screen tip** needs two things first:
  - **Show me how:** a button in the tip that reloads the page with the list in its address, then shows the Share-menu steps, so the new app starts with the list.
  - **Paste a list:** a way to get a list someone texted you into the Home Screen app, since links open in Safari.
  - **Decide about Clear list there:** a Home Screen app saved with the list in its address (which Show me how would do) brings that list back, with Undo, every time it starts fresh after Clear list. That's the "an older copy still restores" rule; the app may need an exception.
- **iPhone Safari bookmarks** may save the loaded address the same way Add to Home Screen does. If so, a bookmark on iPhone won't always carry the list. Test before relying on it.
