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
