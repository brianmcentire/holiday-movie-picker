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
  - **Shift-click a rating or a holiday intensity to show only that one** in its group (the owner's request, Nov 11). Shift-click it again, while it's still the only one on, to put that group back as it was. Shift-clicking another button in the same group moves the solo there and still remembers the original. Changing the group any other way forgets it. Each group remembers on its own. Needs a Shift key, so laptops in practice; there's no hint on the page, and it's only remembered until the page reloads.
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
- **Search finds people:** actors, directors and writers.
  - **Matching:** a surname matches as it's typed ("ferr" finds Elf through Will Ferrell), and so does the part of a surname after a hyphen (Louis-Dreyfus). A full name matches once there's a letter after the space ("will f"). A first name alone never does, so "jack" and "holly" stay about the movies.
  - **Films found only through a person come after the rest**, under the same sorts as the title boost. So "brown" shows the Charlie Brown films before films with an actor named Brown.
  - **Settings:** `PEOPLE_SEARCH` in index.html. `enabled: false` stops searching people; `last: false` mixes person matches in with the rest. There's no switch on the page: pill space is already tight.
- **Spaces mean every word, anywhere** (`EVERY_WORD` in index.html; built Nov 10). A film matches when the whole search matches, as before, or when every word matches somewhere: the title, a person, a tag or the description. Before, a space meant "these words together, in this order", so "rankin bass" and "elf ferr" found nothing; now they find 16 Rankin/Bass specials (with the default filters) and Elf and Saving Santa. "hanks ferrell" still finds nothing, honestly: they've never shared a holiday film.
  - **Neighbouring words can match together.** Required, not optional: word by word, "tom hanks train", "will ferrell elf" and "bill murray ghost" would find nothing, because a first name alone never matches a person. Read together, they find The Polar Express, Elf and Scrooged. Each film splits the words into as few groups as it can.
  - **Whole-search matches come first, in every sort and direction**, then the every-word ones. So old shared links show what they showed, followed by any extra films. The title boost and the people-last order apply within the whole-search ones, as before.
  - Cost: searches that name a title get a longer tail ("christmas vacation" 33 → 66 with the default filters), always after the real matches.
  - Speed: the groups of words are looked up once and kept while typing; at a simulated phone's speed the search part stays under about 25ms a keystroke. Drawing the results takes longer than searching, and typing waits 120ms before redrawing.
- **Who's searchable** (the settings at the top of `data-builder.py`):
  - **Actors:** the first 5 billed; the first 10 for films with 500+ TMDB votes; and anyone billed lower with TMDB popularity 1.5 or more (John Candy is 11th in Home Alone).
  - **Directors:** all of them.
  - **Writers:** the author of the book it's based on (Charles Dickens, whose popularity is only 0.9), and screenwriters with popularity 1.5 or more (John Hughes).
  - **Producers aren't searchable.** Finding a film through someone who isn't in it confuses, and most well-known producers also act, direct or write.
  - In Nov 2026 that was about 8,500 names, taking the compressed download from 246KB to 324KB.
  - **Why a sliding limit:** big films have well-known people deep in the billing (John Krasinski is 15th in The Holiday), while small films are bit parts past the leads. Of 16 well-known supporting actors, top 5 plus popularity found all 16 for 60KB; a fixed top 10 found 13 for 94KB, and a top 20 found 15 for 144KB. The 500-vote line (127 films) adds people like June Squibb and Kris Kristofferson for 1.5KB.
  - A numbered table of names would have saved only 3–4KB: compression already shortens repeats, and 83% of the people appear in just one film.
- **The Holiday Intensity names aren't searchable.** Every film carries one as a hidden tag ("holiday story"), so "story" found 1,016 films. They're filters, not words about the film.
- **Nothing found, but hidden:** when the only matches are hidden by Content Ratings or Holiday Intensity (searching "die hard" with R and It's adjacent off), the empty results say "2 movies match, hidden by Content Ratings and Holiday Intensity." with **Show them**, which turns on every rating (or intensity) and keeps the search. "No movies match your search or vibes." stays grey; the "2 movies match, hidden by…" line is blue, like the note when some results show (the owner's call, Nov 10).
- **Some found, more hidden:** while searching, when Content Ratings or Holiday Intensity hide more matches, a line above the results says so, in blue, with the same **Show them**: "2 more match, hidden by Content Ratings." Why: "bateman" shows Nick and Gabriel Bateman's 3 films, with no hint that Jason Bateman's 2 are rated R. Only while searching; `MORE_HIDDEN_NOTE` in index.html turns it off. *Its wording and place were Claude's call, built overnight on Nov 9 from the agreed plan. The owner kept it and asked for it in blue on Nov 10, so it gets noticed.*
  - **The same note when the hidden ones are the closer matches.** "bad santa" with R off shows 7 films that mention both words, under "3 more match, hidden by Content Ratings."; the hidden 3 are the actual Bad Santa films. No special wording for that. *The owner's call, Nov 11: fine as is.*
- **The title count** ("More than 1,000 titles") is grey at first and turns the tagline's blue once the number changes, then back to grey when it says the same as the unfiltered page again. A filter that doesn't change the number (turning R on still shows "More than 1,000") leaves it grey. Why: "34 titles" is the answer to what you just did, so it should draw the eye. *First built as "blue whenever a filter is on"; the owner wanted it tied to the number.*
  - On laptops it sits on the same line as "a holiday movie matchmaker", right-aligned against the search bar with a tab's width (32px) between them. It floated in the middle of the bar, which looked odd.
  - It keeps the width of "More than 1,000 titles" whatever it says, so the search bar never shifts while typing.
- **The search hint** is the longest that fits the box: "Search titles, actors, keywords...", then "Titles, actors, keywords...", then "Titles, actors...", then "Search". Phones held upright always say "Search". Why: laptop windows under about 1,150px (the count takes room beside the bar), iPads held upright and phones turned sideways showed "Search titles, ac". *The owner asked for a shorter hint there on Nov 10; the middle step was Claude's addition.*
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
- **The movie preview (the peek):** a tap or click anywhere on a card opens it (poster, title or description), and so does Enter on a card. It has the poster, the basics and the overview, plus:
  - **Add to My Watchlist** (or, when the movie is on the list, **Watched it** and **Remove**), the only button;
  - **the rating opens TMDB.** "6.7/10" is TMDB's score, so tapping or clicking it opens the film on TMDB in a new tab, and the preview stays open. There's no hint beyond a pointer and an underline on hover. The owner's browser script makes its IMDb and Rotten Tomatoes scores work the same way.

  Why: an empty bookmark on every tile was clutter. Tapping a poster is what everyone does anyway, and phone tiles hide the overview, so the peek gives something back. The cost is one extra tap to reach TMDB.
  - **More on TMDB was removed** on Nov 11 (the owner's call: the buttons for TMDB, IMDb and Rotten Tomatoes all looked cluttered). Add moved half a button (23px) further from the description on phones: 41px, and 35px on a phone turned sideways. On laptops, the panel shares out the room the button left, so Add moved down about 28px for most films. Films whose long description fills the panel keep 20px.
- **Phones:** the preview is a sheet from the bottom over the dimmed page, and a swipe down closes it.
- **Laptops and tablets** (720px wide and 560px tall or more): the same preview as a large panel in the middle of the screen, 720px wide, over the same dimmed page.
  - The poster is on the left. The full description shows.
  - Add to My Watchlist (or Watched it and Remove), 280px wide, centered in the space beside the poster. Left-aligned under the description, it looked off-center. The owner's call on Nov 10.
  - ×, Esc or a click outside closes it.
  - Why: it matches the phone, which the owner liked, and it gives laptops Add to My Watchlist and the "why it matched" line. Large and centered reads clearly as its own layer and never moves around.
- **Adding on laptops:** the bookmark appears only on the poster under the pointer, and clicking it adds the movie. A click anywhere else on the card opens the preview. Cmd-, Ctrl- or Shift-click or a middle click opens TMDB in a new tab, as links do.
  - Why the whole card: on Nov 10 the owner found the description going straight to TMDB while the poster opened the preview, and called that wrong. One card, one action.

### Why it matched

While searching, the preview says why the movie is in the results. The result cards never change: a line on every card would make rows uneven and the results taller.
- **Up to two lines, under Rating and Score.** Each word, or group of words read together, gets one reason, in this order:
  1. **The title matches:** nothing. The title already explains it.
  2. **A person:** their icon and name, with the matched part underlined (Will <u>Ferrell</u>). If several match: a director first, then the highest-billed actor, then a writer. Why directors first: "zemeckis" named Leslie Zemeckis, who has a small part in The Polar Express, instead of its director Robert Zemeckis. It also makes "favreau" in Elf the director, not the 12th-billed actor.
  3. **A tag or genre:** the tag icon and the tag, plain (🏷 snowman).
  4. **Only the description, past the part that shows:** "Matched description", with a lines icon. Phones show 6 lines of the description, or 3 sideways; laptops show it all, so laptops never need it.
- **One kind per line,** on phones and laptops alike: people on line 1 (directors, actors and writers together), tags on line 2. "rankin bass stop motion" is 🎬 Arthur <u>Rankin</u> Jr. · Jules <u>Bass</u>, then 🏷 stop motion. With only one kind, it continues onto line 2, lined up under the names. *The owner's calls, Nov 10: tags on their own line on laptops, then the same on phones.*
- **One icon per run of the same role:** 🎬 Rankin · Bass share one; 👤 Michael Caine · ✒ Charles Dickens get one each.
- **Matched description** shows whenever the matched word is past the part of the description that shows, but never on a third line:
  - on its own line when it's the only reason ("workaholic" in Elf, or "snow job" in Snow Bride, whose title takes "snow");
  - on line 2 when line 1 holds the people or the tags ("ferrell dad" on a phone: Will Ferrell, then Matched description);
  - dropped only when both lines are already taken, by people and tags, or by a list of people that continues onto line 2. The yellow highlight still marks the word wherever the description shows it.
  
  The owner's call: it never pushes onto a third line, so with title, person, tag and description all matching, the description is the one left out.
- **Two lines at most, and nothing cut short:** no "…" and no partial names. Whatever doesn't fit is left out whole, people first and then in the order typed. Why (the owner): the line is there so people aren't left wondering why a movie came up; when several reasons apply, they don't need all of them.
- **A first reason too long for a line on its own wraps onto line 2** instead, since it may be the only reason. Only three tags are that wide on a phone, all "relationship" tags ("older woman younger woman relationship" on Carol); no person's name is. *The owner's call, Nov 10.*
- **"new york" is the tag *new york city*, not Bob Newhart for "new".** Newhart only matches because his name starts with "new", so naming him would be a misleading reason. Someone who means him can type "newhart".
- **In the description,** the matched words are highlighted in bright yellow (`#fde047`) wherever they show, for every word that didn't match the title.
- **Four icons:** a person for actors, a clapperboard for directors, a pen for writers (book authors too), a tag for tags and genres.
- The lines are fitted to the preview's width when it opens, and again if the window changes size.

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

### Add to Home Screen (parked)

- **Parked on Nov 11, the owner's call:** the fixes it needs are too much for a simple, fun site. The tip stays off; its code stays in place.
- **The Home Screen tip is built but switched off** (`HOME_SCREEN_TIP` in `watchlist.js`). Two findings on iOS 27 made it a poor promise:
  - **iPhone saves the address the page was loaded with,** not the one the page updates afterwards. A list built after the page loaded isn't in that address, so the Home Screen app starts empty. A page reloaded with the list in its address does carry it (tested on the simulator).
  - **A Home Screen app keeps its own storage, separate from Safari's,** and links someone texts you always open in Safari. People who use both would have two lists drifting apart.
- **Picking it back up would need:**
  - **Show me how:** a button in the tip that reloads the page with the list in its address, then shows the Share-menu steps, so the new app starts with the list.
  - **Paste a list:** a way to get a list someone texted you into the Home Screen app, since links open in Safari.
  - **Decide about Clear list there:** a Home Screen app saved with the list in its address (which Show me how would do) brings that list back, with Undo, every time it starts fresh after Clear list. That's the "an older copy still restores" rule; the app may need an exception.
- **If it's back, the tip will read:** "Added Klaus." then, on its own line, "Add Season's Screenings to your Home Screen: open Safari's Share menu ⬆ and choose Add to Home Screen."
  - It names Safari's Share menu, with Safari's icon, because Add to Home Screen exists only there. Our Share results button can't do it.
  - The planned places are once right after the very first add, and at the bottom of the list on phones. It's iPhone Safari only, and never shown once closed or once the site is on the Home Screen.
- **The app name is "Season's Screenings"** (the manifest's `short_name`), and the manifest has no fixed start address.
- **A Home Screen app's launch address only fills an empty list.** It always opens at the address it was added with, so any list in that address (the backup copy or a shared copy) is a snapshot from then. It's never shown as "Shared with you", and it never reopens My Watchlist at launch.
- **Why it would matter:** opened from the Home Screen, the site runs full screen without Safari's bars, and Safari doesn't clear its storage after 7 days without a visit. The list in the address already helps with the 7-day clearing in Safari, since reopening the tab or a history entry brings it back.

### Toasts

- Notes that are sentences or instructions stay on screen twice as long as the short "Removed X · Undo" ones, and have an × to close them early.

## Outside scores (the owner's browser script)

- **`scores.json` is for the owner's Tampermonkey script, not the site.** The page never loads it, so other visitors see and download nothing new. It's public in the repo and at the site's address, though, and holds IMDb and Rotten Tomatoes numbers that MDBList gathers. *The owner accepted that gray area on Nov 11: it's quiet, not private.*
- **Where the numbers come from:** [MDBList](https://mdblist.com), a free hobby service, with the owner's own key (`MDBLIST_API_KEY`). Per film:
  - the IMDb rating (out of 10), the Tomatometer and the Popcornmeter (percents);
  - the IMDb id and the Rotten Tomatoes address;
  - the date its scores were last fetched.
  When MDBList lacks an IMDb id or RT address, Wikidata fills it in.
- **0 means unknown.** MDBList reports a missing score as 0 or null, so a 0% is treated as no score, as are values out of range. A missing field in the file means unknown; the file never holds nulls, zeros or empty strings.
- **Old values are kept rather than lost** (the owner: "if we have old scores id prefer to keep using them than deleting"):
  - a score MDBList leaves out this week keeps last week's value;
  - a film whose request failed keeps its whole entry;
  - no key, a refused key, MDBList being down or every request failing leaves the file untouched;
  - a film that leaves the catalog leaves the file.
- **Polite to MDBList,** so the key never gets blocked:
  - 50 films a request, about 23 requests for the whole catalog, one every 1.5 seconds;
  - at most 300 requests a run, and it stops early rather than leave fewer than 100 of the day's free 1,000, which the browser script also uses;
  - "too many requests": waits as long as MDBList says (up to 2 minutes), twice at most, then stops for the week. The daily limit stops it at once;
  - a server or network error: one retry 5 seconds later, then that batch is skipped. 3 failed batches in a row end the run.
- **It runs every Monday after the movie data,** as its own step that can't stop the data from publishing. `make scores` runs it by hand.
- **MDBList fills gaps lazily.** On Nov 11 a first request for It's a Wonderful Life had no RT scores; the full run minutes later had 94% and 95%. Some well-known films (The Grinch, Red One, Violent Night) still had no Tomatometer, and may fill in on a later run.
- **MDBList can also wipe scores it had.** When it refreshes a film and fails to read Rotten Tomatoes, it saves blanks over the old numbers: It's a Wonderful Life lost both scores and its RT address at 19:25 UTC on Nov 11 (its "updated" time). Popular films refresh most often, so they're hit most. Keeping old values covers it.
- **The script** (`userscripts/scores.user.js`) adds "IMDb 7.1 🍅 85% 🍿 79%" after the rating in the preview (the owner picked emoji, option A on the canvas's Round 10).
  - It reads `scores.json` once per page, so scores show at once. It asks MDBList live only for a film missing from the file, or when the file can't be read, and only with a key set. A gap in the file isn't filled live, since the file came from MDBList anyway.
  - A score with no valid number shows nothing, no icon. With nothing valid, nothing shows at all.
  - The IMDb score opens IMDb, and 🍅 and 🍿 open Rotten Tomatoes, in a new tab, when the file has the address. Otherwise they're plain text.
  - Old scores still show; hovering one gives its source and date ("MDBList, updated Nov 11").
  - The key is set by clicking "Scores: add your MDBList key" in a preview, or from the Tampermonkey menu, and stays in Tampermonkey's storage.

### Finding gems (by hand, when wanted)

A way to rank the catalog by what audiences think, used on Nov 11. It's not a feature and there's no script in the repo: paste the snippet below into a terminal in this folder.

- **The two scores:** the IMDb rating and the Popcornmeter (Rotten Tomatoes' audience score). The Tomatometer is left out: it's critics, and most TV movies don't have one.
- **Why not a plain average:** they're different kinds of numbers. IMDb is an average out of 10 that mostly falls between 5 and 7.5; the Popcornmeter is the share of the audience who liked it, and runs from 7% to 100%. Averaged as is, the Popcornmeter would decide almost everything.
- **Instead:** for each score, the percent of the catalog a film beats (ties count half), and the average of the two. A 96 beats 96% of the catalog on average. A second standard way (averaging how far each score is above the catalog's typical one) picked 19 of the same top 20.
- **Enough votes to trust:** films with fewer than 30 TMDB votes are left out (about 576 films remain with both scores).
- **The knobs:**
  - `FROM_YEAR`: the earliest release year, `0` for any. With any year, the list leans to old classics;
  - `MAX_VOTES`: `600` gives hidden gems only, leaving out the best-known 15% or so (It's a Wonderful Life, Klaus, Die Hard); `None` doesn't penalize popular films.
- **A bias:** TV movies with few votes (the Hallmark kind) are rated mostly by fans, so their scores run high.
- **What came out (Nov 11):**
  - hidden gems, any year: The Snowman, The Thin Man, Scrooge (1951), The Lion in Winter, Emmet Otter's Jug-Band Christmas;
  - 2010 on, popular films included: Klaus, The Holdovers, Little Women (2019), A Biltmore Christmas, Boiling Point.

```sh
python3 -I - <<'EOF'
import json
MIN_VOTES, MAX_VOTES, FROM_YEAR, TOP = 30, None, 2010, 20   # MAX_VOTES=600 for hidden gems only
LEVEL = {'about': 'story', 'set_at': 'setting', 'adjacent': 'adjacent'}
db = json.load(open('holiday_engine.json'))['movies']
sc = json.load(open('scores.json'))['movies']
lv = json.load(open('holiday_levels.json'))
pool = [dict(db[i], **sc[i], level=lv.get(i, {}).get('level')) for i in db
        if sc.get(i, {}).get('imdb') and sc[i].get('popcorn') and db[i]['votes'] >= MIN_VOTES]
for key in ('imdb', 'popcorn'):  # percent of the pool each film beats, ties counted as half
    vals = [m[key] for m in pool]
    for m in pool:
        m['p_' + key] = 100 * (sum(v < m[key] for v in vals) + (vals.count(m[key]) - 1) / 2) / (len(vals) - 1)
for m in pool:
    m['score'] = (m['p_imdb'] + m['p_popcorn']) / 2
picks = [m for m in pool if m['date'][:4] >= str(FROM_YEAR) and (MAX_VOTES is None or m['votes'] < MAX_VOTES)]
for n, m in enumerate(sorted(picks, key=lambda m: -m['score'])[:TOP], 1):
    print(f"{n:2}. {m['title']} ({m['date'][:4]}) | IMDb {m['imdb']} | 🍿 {m['popcorn']}% | 🍅 {m.get('tomatoes') or '–'}"
          f" | {m['cert']} | {LEVEL.get(m['level'], '?')} | {m['votes']} votes | {m['score']:.0f}")
EOF
```

## Musical pill (planned, not built)

Decided with the owner on Nov 11; building waits for the owner's go.

- **TMDB's "Music" genre pill becomes "Musical",** filled from Claude's answer instead of TMDB's. One pill, not two. It works like the other genre pills: include, exclude, reset. Mostly it's for leaving musicals out, and some people will want only them. No old links need to keep working; nobody but the owner uses the pill yet.
- **Why not an existing label:** none is reliable. TMDB has no Musical genre (its "Music" genre lumps musicals in with concerts and films about musicians), and its user-added "musical" tag misses Rudolph (1964) and Emmet Otter while tagging The Lion in Winter. IMDb's and MDBList's Musical labels miss Rudolph (1964), The Year Without a Santa Claus and Nativity!. All three were checked on Nov 11.
- **What Claude is asked:** which of four kinds a film is, plus a rough count of full songs performed on screen:
  - **musical:** several full songs or song-and-dance numbers performed on screen that carry the story or are the point of it, animated specials included when they're built around their songs;
  - **music show:** a concert, a variety special built on songs, a filmed ballet or opera, or a documentary about musicians or a song;
  - **some songs:** a song or two, a carol, a pageant number or one performance, in a story told mostly in dialogue;
  - **none.**
- **The pill holds musicals and music shows.**
  - A romance about a singer is in only if it's full of songs (the owner's call).
  - Frosty the Snowman is out (the owner: "close but no").
  - Christmas specials built on songs, like Rudolph (1964) and The Year Without a Santa Claus, are in. The owner will turn the pill back on when wanting them.
- **Test round (Nov 11, Opus at low effort):** 20 of 20 hand-labeled edge cases right.
  - In: White Christmas, Holiday Inn, Meet Me in St. Louis, The Muppet Christmas Carol, Spirited, Jingle Jangle, The Nightmare Before Christmas, Rudolph (1964), A Very Murray Christmas, the Sabrina Carpenter and Mariah Carey specials, and Wham!: Last Christmas Unwrapped.
  - Out: Elf, It's a Wonderful Life, The Polar Express, A Charlie Brown Christmas, Love Actually, Frosty, The Lion in Winter and The Humans.
  - Unlabeled ones landed: Nativity!, the 1998 Rudolph movie, The Year Without a Santa Claus and Emmet Otter as musicals; Rudolph (1948), Christmas Harmony, Holiday Harmony and A Cinderella Story: Christmas Wish as some songs.
- **Models compared** (Nov 11, low effort, the same 28 films, run several times each):

  | Model | Labeled films right | Whole catalog once | With the Batch API |
  |---|---|---|---|
  | Opus | 20 of 20 (2 runs) | about $8.50 | about $4.25 |
  | Sonnet | 20 of 20 (3 runs) | about $3.90 | about $1.95 |
  | Haiku | 19 of 20 (3 runs) | about $0.22 | about $0.11 |

  - Haiku called Frosty a musical every time, with 5 to 8 songs (it has about 2), and changed its mind about A Cinderella Story: Christmas Wish between runs.
  - Sonnet agreed with Opus on the unlabeled films. Holiday Harmony went back and forth between runs; it's a true borderline case.
  - The Batch API halves the price. Most batches finish within an hour, and the limit is 24 hours. That's fine for the one-time run; the weekly new films use normal requests, since the saving is pennies.
- **Answers are saved per film,** like holiday levels, so each film is asked once and later runs cost pennies.
- **The one-time run over the whole catalog** (the owner's plan, Nov 11):
  1. Opus and Sonnet each answer every film, as batches. Each gives the kind, the song count, its confidence, and whether it knows the film.
  2. Fable decides a film when the two disagree on in or out of the pill, or when either says low confidence or doesn't know the film. Disagreeing only on musical vs. music show isn't a disagreement. Medium confidence doesn't count: in the test it covered a quarter of the films, easy ones included.
  3. **A pause before Fable:** the count of films for Fable and its estimated cost are reported first, and Fable runs only on the owner's go. If the count is too high, films where both models say "none" and TMDB has no musical tag or Music genre are skipped.
  4. A review list shows what Fable decided and why, marking films that no model knew.
  5. Then a spot check of about 20 random films in the pill, since two models that agree can still share a blind spot.

  Estimated at $8 to $15 in all. **Weekly runs are undecided** until the one-time run shows whether the extra steps earn their keep.
- **Not used:**
  - curated lists: none found on GitHub, and Wikipedia's lists (behind the Wikidata check) barely cover TV specials and TV movies;
  - a trained classifier like BERT: it needs hundreds of labeled films and sees only the description, which rarely mentions songs;
  - the new "System One" decision models: early access, weak at counting by their own explainers, and nothing to save on about 1,100 films.

## Decided against

- **Laptop list:** a side drawer and a centered panel. My Watchlist is its own page.
- **Look of the marks:** checkmarks on the results; a watched sash alone; fading alone. The popcorn icon, plus heart, star, ticket, clock and plain words.
- **Ways to add:** an empty bookmark on every tile; long-press to add (hidden, and it fights iPhone's own preview); double-tap to add (hidden, and it slows every tap); an "add mode" switch.
- **Top of the page:** an Unwatched | Watched | All switch, and a My List row under the search box.
- **Names for Holiday Level:** Holiday Spirit (reads as mood: a gloomy film set at Christmas still counts as "the setting"), Tinsel Factor (Christmas-only, and some films are Hanukkah, New Year's or Thanksgiving), and the question-style names.
- **Search order:** every title match first ("christmas" put It's a Wonderful Life 657th, "santa" put Klaus 56th); only exact titles first (too narrow); whole words first while keeping partial-word matches (Elf stayed 11th).
- **Why it matched:**
  - a line on every result card, which gave uneven card heights and taller results;
  - a badge on the poster: the corners are taken, and an icon alone leaves you guessing;
  - words instead of icons ("from the novel by Charles Dickens" wraps on a phone);
  - a yellow highlight on people and tags;
  - naming every reason one word matches (title, person and tag for "snow"); each word, or group read together, gets one.
- **The laptop preview:** a popover next to the clicked card. It's fiddly near the screen's edges, and it moves around.
- **Links to TMDB, IMDb and Rotten Tomatoes under Add** (canvas Round 11, Nov 11): a "More on:" line of text links, the same line in small capitals, and rows of equal buttons with each site's name and the arrow. All looked cluttered; the rating links to TMDB instead.
  - *If buttons ever come back, the owner's pick is E:* plain buttons like the old More on TMDB, equal widths, labeled TMDB, IMDb and "Tomatoes", with slightly tighter letter spacing so "Tomatoes" fits a laptop's 88px button. "Rotten Tom." doesn't fit, and "RT" is less known.
- **People search:** a fixed top 10 or 20 actors per film; producers; a switch on the page to turn it off.
- **The reason line for several words:** "…" or partial names when it doesn't fit; people and tags mixed on one line; naming every person whose name starts with a typed word (Bob Newhart for "new" in "new york").
- **The title count:** blue whenever any filter is on. It follows the number instead, so turning R on (still "More than 1,000") stays grey.

## Future polish

Ideas for sharing a list and not losing one, liked by the owner on Nov 11 but not scheduled. Ranked, best first.

Where a list gets lost today: closing a tab in a normal window keeps it. It's lost in a private window, in an app's built-in browser (Instagram, Facebook, WhatsApp and the like keep their own storage), after Safari goes 7 days without a visit (unless it's reopened from a bookmark or history), and on another browser or device.

1. **Movie titles in the share text:** "My watchlist from Season's Screenings: Elf, Klaus, The Holdovers and 6 more", with the link. It reads at a glance in the chat, turns up when searching messages later, and still helps if the link breaks. Small.
2. **Say a link is a copy,** in the "Link copied" note and on the "Shared with you" card: "They get their own copy; changes don't sync. Ask them to send theirs back." Couples may expect a shared list to stay in step. Tiny Little Office (tinylittleoffice.com, also "the link is the document") added the same line after a group sent a sign-up sheet to 20 people and got 20 separate copies back. One sentence.
3. **A note in app browsers:** when the page opens inside Instagram, Facebook or similar (their browsers name themselves), a quiet line: "Your list may not stay here. Open in Safari." Small.
4. **One reminder to save a copy,** once the list reaches about 5 movies and never again: "Your list lives in this browser. Save a copy to keep it safe." Small, but it could feel naggy.
5. **"Send yours back"** on a list someone sent you, sharing the combined list in one tap. Medium.
6. **Remember lists people sent you,** so a list opened with Just look can be found again without digging up the message. Medium.

Not on the list: short links, a preview picture per list, and syncing between devices. Each needs a server or accounts; lists are short enough as links already.

## Revisit later

- **Films without a holiday level** (when the classifier couldn't run) are hidden by the default Holiday Intensity, though the footer mentions them. They show with Any holiday intensity. Fine as a safe default, or let them through.
- **TV-G, TV-PG and TV-Y7:** the builder allows them but Content Ratings has no buttons for them, so such a film would be hidden by default and couldn't be picked. None in today's data.
- **Pill text contrast:** white 10–11px text on the blue, green and red pills is about 3.3–3.8:1, under the 4.5:1 guideline for small text.
- **The Undo after tapping a bookmark** may be redundant, since tapping the bookmark again removes the movie. Kept for now as feedback.
- **"Mark them watched too"** in Just look, for couples who watch together while only one of them marks things. Not built.
- **Mail's subject line:** check on a real iPhone whether Mail uses the share title as the subject.
- **People search drifts a little week to week:** TMDB's popularity scores change, so a borderline supporting actor can drop in and out between rebuilds.
- **Classic casts thin out past the 10th billed:** old stars score low on TMDB popularity, so supporting players further down are missing (Jack Albertson, 14th in the 1947 Miracle on 34th Street).
- **iPhone Safari bookmarks** may save the loaded address the same way Add to Home Screen does. If so, a bookmark on iPhone won't always carry the list. Test before relying on it.
