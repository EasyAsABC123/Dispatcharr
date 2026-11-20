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
