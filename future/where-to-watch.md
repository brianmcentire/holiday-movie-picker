# Where to watch (a future option)

Designed with the owner on Nov 17, 2026. **Not built:** the owner chose to leave it for later. The "Picked" column below has the owner's choices; the alternatives stay here in case they reconsider. If it's built, move the settled parts into [DESIGN.md](../DESIGN.md) and delete this file.

## The idea

The movie preview shows where the film streams in the US:
- Up to three service logos, biggest service first.
- A grey "+2" when there are more.

The whole row links to the film's Where to Watch page on TMDB, which lists every option and links into each service.

The result cards don't change (they stay fixed-height), and nothing goes in the page's address, so shared links aren't affected.

Nothing has been mocked yet. Following the usual order, mock it on the design canvas before building, with a few films from the examples below.

## Decisions

| | Decision | Picked | Alternatives |
|---|---|---|---|
| 1 | Where | The movie preview: a row under the scores with a small "Streaming" label | Also My Watchlist's rows (taller rows). A "My services" filter (medium size, and a new part of the link format, which would then have to keep working forever) |
| 2 | Form | Logos: the 3 biggest services, then "+N" | Names as text ("Streaming on: Netflix, Disney+"). Every logo (up to 9 on one film) |
| 3 | Tapping | The whole row opens TMDB's Where to Watch page in a new tab, leaving the preview open | "+N" shows the rest in the preview. Each logo links to its service (TMDB's API doesn't give those links; TMDB's page has them) |
| 4 | What counts | Included with a subscription, or free with ads | Rent and buy too: 68 films have only those. A small "Rent or buy" link to the same page would cover them |
| 5 | Grouping | One logo per thing you'd pay for. A service's plans share one logo (Netflix Standard with Ads becomes Netflix). Add-on channels sold through Amazon, Apple TV or Roku count as the service itself | JustWatch's own list, one logo per listing: Hallmark+ would appear three times on 295 films |
| 6 | Lifetime | Lifetime's free app and Lifetime Movie Club share one logo (18 films are on both) | Two logos |
| 7 | Same owner | Not merged: Hulu stays apart from Disney+ (9 films are on both), and Cinemax from HBO Max. Each is sold separately | Merge Hulu into Disney+, for example if Disney folds Hulu in completely |
| 8 | Live-TV bundles | Left out: Philo (372 films), Fubo (371), YouTube TV (23). They carry the Hallmark Channel, so they'd be on every Hallmark film | Rank them last. Or show them only when there's nothing else |
| 9 | Channel apps that need a cable login | Left out: TNT, TBS, truTV, Freeform, AMC, DisneyNOW, The CW | Include them |
| 10 | Library-card services | Left out: Hoopla (359 films), Kanopy (65) | Rank them last, as "free with a library card" |
| 11 | Small services | Left out: about 60 that most people haven't heard of, such as Fawesome (104 films) and Amazon MX Player (66). Only the 21 below show | Show them after the 21 |
| 12 | Order | A hand-kept list, roughly by US audience (below) | TMDB's own order, which can't be used: it ranks HBO Max 155th but HBO Max's Amazon channel 15th, and Tubi 347th. Alphabetical |
| 13 | Free services | Ranked by their audience along with the paid ones, so Tubi comes before Apple TV | Paid services first. Or save one of the three spots for a free service when there is one (4 films today) |
| 14 | Films not out yet | Show the service named in TMDB's US release note (Netflix for Clashing Through the Snow) until JustWatch lists the film | Show nothing until it streams (15 of this year's 31 films today) |
| 15 | Hallmark+ logo | Crop it to the crown and plus sign. TMDB has only the Amazon and Apple channel versions, which carry a "prime video \| CHANNELS" band | Keep a small copy of Hallmark+'s own logo in the repo. Or show the channel tile as it is |

## The services, biggest first

Several services no longer publish subscriber numbers, so the order in the middle is a judgment call. Check it each fall.

| | Service | TMDB listings it covers | Logo from TMDB provider |
|---|---|---|---|
| 1 | Netflix | Netflix, Netflix Standard with Ads, Netflix Kids | 8 |
| 2 | Prime Video | Amazon Prime Video, Amazon Prime Video with Ads, Amazon Prime Video Free with Ads | 9 |
| 3 | YouTube | YouTube Free (free with ads) | 235 |
| 4 | Disney+ | Disney Plus | 337 |
| 5 | HBO Max | HBO Max and its Amazon channel | 1899 |
| 6 | Hulu | Hulu | 15 |
| 7 | Paramount+ | Paramount Plus Premium and Essential, and its Amazon and Roku channels | 2303 |
| 8 | Peacock | Peacock Premium and Premium Plus, and its Amazon channel | 386 |
| 9 | Tubi | Tubi TV | 73 |
| 10 | The Roku Channel | The Roku Channel | 207 |
| 11 | Apple TV | Apple TV and its Amazon channel | 350 |
| 12 | Pluto TV | Pluto TV | 300 |
| 13 | Plex | Plex | 538 |
| 14 | Starz | Starz and its Amazon, Apple TV and Roku channels | 43 |
| 15 | Fandango at Home | Fandango at Home Free | 332 |
| 16 | Xumo Play | Xumo Play | 1963 |
| 17 | MGM+ | MGM Plus and its Amazon and Roku channels | 34 |
| 18 | AMC+ | AMC+ and its Amazon, Apple TV and Roku channels | 526 |
| 19 | Hallmark+ | Hallmark+ through Amazon and through Apple TV, and Hallmark TV through Amazon | 290, cropped |
| 20 | Lifetime | Lifetime Movie Club and its Amazon and Apple TV channels, plus Lifetime's own app | 284 |
| 21 | BritBox | BritBox and its Amazon and Apple TV channels | 151 |

- **Logos:** each is a square app-icon tile, at `https://image.tmdb.org/t/p/w92` plus the provider's `logo_path`, from TMDB's US list (`/watch/providers/movie?watch_region=US`). Read the paths from that list on each run rather than writing them into the code; they can change.
- **The channel versions carry the store's band** ("HBO max on prime video"), so a merged service always uses the logo of the service itself.
- **Match the names loosely.** TMDB isn't consistent: it has "Paramount Plus" and "Paramount+", "Britbox" and "BritBox", and one listing with a trailing space. New channel listings keep appearing, so match by name pattern rather than by id.

## Across the catalog (Nov 17, 2026)

Out of 1,038 films:

| Logos shown | Films |
|---|---|
| 1 | 516 |
| 2 | 140 |
| 3 | 46 |
| 3, then "+N" | 111 (+1: 37, +2: 41, +3: 23, +4 to +6: 10) |
| none | 225 |

Of the 225 with none:
- 99 have no US data on TMDB.
- 68 can only be rented or bought.
- 58 stream only on the services left out above.

Hallmark+ is on 295 films. For 237 of them it's the only logo.

Examples:
- Elf: Prime Video, HBO Max
- Home Alone: Disney+
- Love Actually: Netflix, Peacock, Starz
- It's a Wonderful Life: Prime Video, Paramount+, Peacock, +2 (The Roku Channel, Plex)
- Jingle All the Way: Prime Video, Disney+, Hulu, +1 (Peacock)
- The Apartment: YouTube, Tubi
- A Charlie Brown Christmas: Apple TV
- What If Christmas: Hallmark+

## Building it

**Size:** small. The builder needs about 30 lines plus the services table, and the page about 30.

- **The builder already has the data.** `fetch_movie_details` asks TMDB for `watch/providers` for every film each Monday, and the Hallmark and Lifetime rule reads it through `us_channels`.
- **What's new in the builder:**
  - read the flatrate, free and ads lists;
  - group and rank them by the table above;
  - keep each film's services in `holiday_engine.json`, as a few numbers pointing into a list of services stored once (name and logo path). That's a few KB.
- **Films not out yet:** for this year's films with no services, turn the US release note into a service, using the patterns already in the builder (`STREAMER_NOTE`, `TV_NETWORK_NOTE`).
- **The page:**
  - a row in the preview after the scores, with logos as 28px rounded squares loaded from TMDB's image server like the posters;
  - each logo's alt text is the service's name;
  - the row links to `https://www.themoviedb.org/movie/{id}/watch?locale=US` (TMDB forwards that to the film's full address);
  - like the scores, a film with no services shows nothing.
- **A module that can come back out,** per DESIGN.md's ground rules: its own file, or next to the scores in `watchlist.js`.
- **The JustWatch credit TMDB requires** is already in the footer ("Streaming data from JustWatch").

## Limits

- **US only.**
- **Only as fresh as the Monday refresh.** Services move holiday films around all season, but the linked TMDB page is always current.
- **A logo doesn't say how it's offered.** The Prime Video logo covers films included with Prime and films free to everyone with ads. Nothing marks a service as free or paid; TMDB's page shows that.

## Where the numbers came from

On Nov 17, 2026, a one-off script read `/movie/{id}/watch/providers` for each of the 1,038 films, plus TMDB's US provider list. The script wasn't kept; a fresh pull is a few minutes' work.
