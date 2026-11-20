# Original browsable programme list over TCP

The original Dispatcharr, Django and Django REST Framework contributors retain full application and workflow credit.

The pinned Original returned HTTP 200 for an owned admin's JSON and HTML
requests to `/api/epg/programs/` through the Django uWSGI TCP listener in
an isolated Compose project. The JSON body was 275 bytes. The HTML body
was 16,069 bytes and its displayed JSON matched the JSON response. The
page title was **Program List – Django REST framework**, with a **Program
List** heading, two preformatted blocks, and two forms each for GET and
POST. Both responses advertised `GET, POST, HEAD, OPTIONS`. The HTML
response also varied on Cookie.

Both requests preserved the owned programme rows. The controller exited 0
and verified retirement of both owned generations and empty Compose
inventories. The receipt retains selected headers, body digests and the
document structure. The request credential and raw response bodies stayed
inside the guest.

The exact Rust `e7651a6d` programme handler still refuses this HTML
representation. This evidence covers one paired representation check on
Original. Rust rendering, form submission, programme creation, other guide
routes and performance remain open. For the Rust list GET, reuse the
existing authorization, bounded database read and typed programme response.
Qualify the rendered representation and any advertised write actions
against their own normal stock behavior.

[Redacted Original receipt](../scripts/rust-epg-contracts/verified-original-browsable-programmes-1.json)

## Packaged Rust successful list GET

The exact `81ce753f` package matched Original on two authenticated admin
HTML GETs to the no-query programme list. Both returned 200 with the same
16,069-byte response length, selected headers and normalized document
bytes. The JSON controls returned the same typed programme data shown
inside each page. Each HTML request generated a fresh CSRF cookie and
mask. Both masks in each response matched its cookie. The receipt
retains the attributes and match results without credential values.

The programme rows stayed unchanged, and Compose verified retirement of
all four owned generations. This verifies the captured successful GET
and JSON control. Programme creation, form submission, alternate methods,
guide navigation and performance remain open.

[Redacted paired Guide receipt](../scripts/rust-epg-contracts/verified-native-browsable-programmes-81ce753f.json)

The exact `81ce753f` image passed 37 existing programme comparisons, covering
36 JSON GET responses and the HEAD representation length. It also passed the
1,024-check packaged baseline across 18 suites and decoded 75 MPEG-TS frames.
Both runs verified teardown. The [e765 coordinator checkpoint](rust-e765-baseline-acceptance.md)
records 17 passes on its own package.

[Programme JSON and HEAD receipt](../scripts/rust-integration/milestone-batch113/programme-json-head-81ce.json)
[Packaged baseline receipt](../scripts/rust-integration/milestone-batch113/81ce-18-suite-baseline.json)

## Other Original Guide representations

Three additional requests returned HTTP 200 from pinned Original through
the isolated Django uWSGI listener. Each HTML response embedded the same
typed programme data as its JSON control using the same actor.

| Request | HTML bytes | Observed document |
| --- | ---: | --- |
| Admin list with `?format=api` | 16,159 | Program List with two POST forms and two masked CSRF slots |
| Standard user HTML list | 11,162 | Program List with one masked CSRF slot and a single form marked GET |
| Admin HTML detail | 18,686 | Program Instance with ten detail-path slots and three populated form-value slots |

The detail response advertised `GET, PUT, PATCH, DELETE, HEAD, OPTIONS`.
The requests only read one owned programme and left the programme rows
unchanged.

The same stock uWSGI listener sent bytes after the HEAD response headers
under a close-delimited HTTP/1.0 request. JSON HEAD returned the same 275
bytes and digest as the matching GET. HTML HEAD declared and sent 16,061
bytes, while the matching GET sent 16,060. Both HEAD responses matched the
corresponding GET status and media type. The fixture read the underlying
stream and verified retirement of both owned Compose generations.

The grouped receipt covers Original. Rust rendering of these variants,
browser workflows and performance remain open.

[Redacted grouped Original receipt](../scripts/rust-epg-contracts/verified-original-browsable-guide-variants-1.json)

## Stock nginx list HEAD

The stock Original AIO nginx listener returned HTTP 200 for the same
authenticated list GET and HEAD requests. Both HEAD responses had no
entity bytes on the raw HTTP/1.0 stream. JSON HEAD declared 275 bytes,
matching the GET. HTML HEAD declared 16,070 bytes, one more than the
16,069-byte GET body. Status and media type matched in both pairs.

The separately verified direct uWSGI listener sent 275 JSON bytes and
16,061 HTML bytes after its HEAD response headers. The nginx run preserved
programme rows and verified teardown. This receipt covers Original stock
front-door framing.

[Redacted stock nginx HEAD receipt](../scripts/rust-epg-contracts/verified-stock-nginx-guide-head-1.json)

## Packaged Rust list variants and HEAD

The exact `11455873` package matched pinned Original for the admin
`?format=api` request with `Accept: */*`, the standard-user bare HTML list,
and the admin bare HTML list. Each variant returned two successful HTML GETs
with fresh masked CSRF values, matching typed JSON controls, and the same
normalized Source template. The programme rows stayed unchanged.

Native admin HTML HEAD matched the separately recorded stock nginx response
on status, selected headers and framing. It declared 16,070 bytes and sent
none on the raw HTTP/1.0 response stream. The matching HTML GET sent 16,069
bytes. The Native JSON HEAD regression declared 275 bytes and sent none.
The same pair's cleanup verified retirement of four owned generations.

This evidence covers the successful requests above on the exact package.
Programme creation, form submission, detail HTML, broader negotiation and
performance remain open.

[Redacted paired list variants and HEAD receipt](../scripts/rust-epg-contracts/verified-native-guide-list-variants-11455873.json)

### Packaged Main regression on the same image

The full 18-suite baseline passed 1,024 matched checks and decoded 75
MPEG-TS frames on `11455873`. The baseline execution, evaluation and
cleanup each exited 0. This qualifies the baseline node on the exact
image. The coordinator exited 2 with the wider 127-node roster open.

[Redacted packaged Main baseline receipt](../scripts/rust-integration/milestone-batch113/114558-18-suite-baseline.json)

### Standard format alias and admin detail on e0a3c10f

Pinned Original and Native `e0a3c10f` returned HTTP 200 for the Standard list
with `?format=api` and `Accept: */*`, and the admin programme detail with
`Accept: text/html` for both a nullable and a populated programme. Each
profile used fresh masked HTML responses and same-actor JSON controls. The
Standard list matched the captured Original template. Both detail profiles
matched the exact Original HTML bytes after substituting the independent
CSRF mask, with the same rich embedded JSON.

The detail forms preserved null and populated field values, all ten
canonical path references, and the ordered raw-form JSON. The populated
row distinguished millisecond truncation from rounding and preserved the
original non-ASCII text in the UTF-8 textarea. Programme SQL stayed
unchanged. The controller exited 0 and independently verified retirement
of all four owned Compose generations.

The first run on `d482e39c` exited 1. The alias, headers, typed JSON, form
values and cleanup matched. Both detail HTML bodies differed because the
rich JSON fields appeared in a different order. Reconstructing the four
retained body hashes offline isolated that difference. The follow-up
restored the Original serializer order and matched the detail bodies on
the new package.

[Redacted Standard alias and admin detail receipt](../scripts/rust-epg-contracts/verified-native-guide-detail-alias-e0a3c10f.json)

### Standard and admin format-alias detail on c6061adb

Pinned Original and Rust `c6061adb` matched the Standard user's bare detail
GET, the admin's `?format=api` detail GET with `Accept: */*`, and the
existing admin bare detail GET. All three returned HTTP 200 and matched
the original HTML bytes after substituting the independent CSRF mask.
Each rendered response embedded the typed JSON from its same-actor
control, with the Original field order in HTML.

The Standard layout contains no editable programme form. Both admin
layouts preserved the Original form fields and canonical detail links.
The fixture left Guide SQL unchanged and independently verified
retirement of all four Compose generations.

[Redacted Standard and admin detail receipt](../scripts/rust-epg-contracts/verified-native-guide-detail-role-alias-c6061adb.json)

### Admin detail and Standard list HEAD on c2a35f9d

The packaged Rust successor matched the pinned stock Original nginx
front door for admin programme detail and Standard programme list
HEAD. The same authenticated GET controls also matched after
normalizing each HTML CSRF mask. Every pair returned HTTP 200 and
matched the media type, Allow and Vary headers.

| Request | Media | GET entity bytes | HEAD declared bytes | HEAD entity bytes |
| --- | --- | ---: | ---: | ---: |
| Admin programme detail | application/json | 774 | 774 | 0 |
| Admin programme detail | text/html | 18,697 | 18,698 | 0 |
| Standard programme list | application/json | 275 | 275 | 0 |
| Standard programme list | text/html | 11,171 | 11,172 | 0 |

Each HEAD was read to connection close at the wire. The controller
matched all four pairs, verified that Guide SQL stayed unchanged and
independently retired all four owned Compose generations.

[Redacted stock nginx and Native HEAD receipt](../scripts/rust-epg-contracts/verified-guide-stock-nginx-head-c2a35f9d.json)

### Standard programme detail format alias on 82414407

Pinned Original and packaged Rust matched the Standard user's detail GET
with `?format=api` under both `Accept: */*` and `Accept: text/html`.
Both returned HTTP 200 with 12,459 HTML bytes. The same Compose pair
matched the Standard bare detail GET and the admin detail alias with
explicit `Accept: text/html`.

Each body matched after substituting its fresh CSRF mask, and the typed
embedded JSON matched its same-user JSON control. The Standard page
displayed no editable programme form. Guide SQL stayed unchanged in
both arms. Independent cleanup verified all four retired generations and
empty owned inventories. [Redacted receipt](../scripts/rust-epg-contracts/verified-native-guide-standard-detail-format-82414407.json)

The original Dispatcharr, Django and Django REST Framework contributors
retain full credit. [Dispatcharr upstream](https://github.com/Dispatcharr/Dispatcharr)
