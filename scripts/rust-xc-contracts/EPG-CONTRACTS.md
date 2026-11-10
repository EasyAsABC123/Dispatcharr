# Original XC short and simple EPG contracts

All application and algorithm credit remains with the original Dispatcharr
authors and contributors. This is source characterization, not native acceptance.
The actual `apps.output.views.xc_get_epg` function ran against original migrated
PostgreSQL/Redis rows at one fixture clock. The first corpus has 750 cases:
359 successful payloads, 326 `Http404` results and 65 `ValueError` results. See
`verified-xc-epg-source-1.json` and its captured tools.

Both actions authenticate through the existing XC API entry point. This helper
then resolves the original `stream_id` with Python `int`, filters hidden channels
and applies channel access without the XMLTV/live-catalog admin level cap.
The raw query spelling remains in the returned `stream_id`. Effective guide/name
and group overrides are honored. Window resolution reuses the XMLTV export
helper, not REST grid/programme windows.

Numbering is a separate source rule: the effective group selects channels by
raw group FK; only hidden channels are excluded from that numbering set.
Integer effective numbers reserve slots before fractional numbers are assigned.
Null numbers are not assigned a slot here; the target falls back to zero.
An overridden channel that moved groups may fall back to its truncated number.
The live-catalog/global XMLTV numbering adapter therefore cannot be reused
without preserving these differences.

Short EPG parses `limit` (default four), selects stored programmes with `end >
now`, sorts by start and applies the slice. Full/simple EPG still parses `limit`
but does not apply it; it uses `end > lookback` and optional `start < cutoff`.
This strict end comparison differs from XMLTV export's inclusive lookback.
Negative limits can fail stored-query slicing while generated dummy limiting
can return an empty list. Invalid integer limit text fails even in simple mode.
A dummy source with stored programmes uses those rows here; XMLTV export's
source-type dispatch is different.

Fields retain source order: id, epg_id, base64 UTF8 title, empty lang, UTC start
and end, base64 UTF8 description, channel_id, decimal timestamp strings,
original stream_id and has_archive. Simple output additionally has now_playing.
Archive uses channel catch-up policy and strict ended/retention comparisons;
now_playing uses closed start/end comparisons. Stored programme IDs are exact.

Generated IDs use `str(abs(hash(channel_id + '_' + timestamp)))`. Two actual
fresh original Python processes produced different IDs for the same inputs,
while the empty string hashes to zero. The corpus also records seven actual
PYTHONHASHSEED=0 vectors covering ASCII, Latin1, BMP and astral text. Any native
comparison must identify generated rows explicitly before normalizing their
process-random IDs; real stored IDs and every other field remain exact.

The first corpus covers five source user roles, hidden and high-level channels,
real/shared guides, null/fractional/zero numbers, overrides, standard dummy guides,
limits, Unicode/alternate integer spellings, duplicate parameters and windows.
Custom regex guides, richer category collisions, archive boundary timestamps,
stored-dummy rows, metadata failures and actual entry-point HTTP/auth/error wire
are additional gates. VOD/series refresh side effects remain separate and must
not be replaced with cached-only read claims.

The staged native reader now matches all 750 actual original helper cases and
88 real HTTP cases. Stored-row JSON bytes and error classes are exact. Only
measured process-random generated IDs are normalized; all other fields remain
exact. The hash primitive also matches seven actual zero-seed vectors. See
`verified-xc-epg-native-2.json` for frozen Rust/tool hashes, logs and cleanup.

The reader performs one atomic scoped metadata SELECT, keeps category numbering
separate from visibility, and projects only the title/description fields needed
from stored programmes. Rows stream into the bounded ASCII JSON writer; it does
not build an entire response Value tree. Byte, row and number-allocation caps
fail explicitly instead of silently discarding programmes. The real fixture
proved cancellation while the metadata SELECT was observed waiting on a
PostgreSQL table lock: admission stayed held until the owned worker finished.
A detached response frame retains its byte budget through its final clone after
shutdown. Row, response, numbering and SQL metadata caps all passed.

App activation follows these component gates; packaged image/middleware proof
remains separate. Custom dummy source properties are explicitly 501 and are not
replaced with fabricated standard programmes. Nonzero configured Python hash
seeds, richer numeric/category/time boundary grammars, and VOD/series refresh
side effects remain independent gates. No resource performance gains are claimed.

Packaged image 24 now passes all 750 helper cases over the actual application
HTTP surface and 88 original HTTP comparisons, using the frozen source/image
identity in `verified-xc-epg-image24-3.json`. Two PostgreSQL metadata queries were
observed waiting while the disposable fixture held the channel table lock.
Closing one caller socket kept both admission slots protected; a third request
returned 503. Unlocking recovered normal responses. The same image with a
64-byte response budget rejected oversized output, and an unverified nonzero
Python hash seed returned 501 for generated programmes. Both containers exited
cleanly without OOM or drain warnings; the fixture was removed.

Two preceding packaged tool failures remain preserved: JSON-array query pairs
required tuples for urllib.urlencode, and the lock-holding PostgreSQL transaction
needed its cached statistics snapshot cleared before each monitoring poll. No
application change or image rebuild was used for either correction. XMLTV's
47-case/cache/audit regression also passed on the same frozen image. These are
scoped functional/resource gates, not a whole-application or performance claim.
