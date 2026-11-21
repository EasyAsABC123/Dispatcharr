# M7 Connect webhook delivery and the Stock live client event

Pinned Original and exact Rust 21514da5 each passed two saved webhook
Test actions through the standard administrator route. One saved
integration was enabled and one was disabled. Each made exactly one
POST to the owned internal receiver. The receiver returned HTTP 200
and 202 on the two tested paths. Both apps returned the expected Test
response and sent the ordered form body with the configured header.
The Test actions left integration, subscription and DeliveryLog rows
unchanged. Both fixture controllers and the independent checker exited
0. The isolated Original and Rust projects verified retirement of two
and four roles respectively.

The separate Stock client_connect witness read 75,200 aligned MPEG-TS
bytes from one authenticated live viewer, with useful non-null packets
and one active origin. Stock emitted one SystemEvent linked to the
viewer and one successful DeliveryLog for the owned webhook request.
The audit preceded the durable log and the origin retired normally.
This Stock result is the reference for the Rust streaming-producer check.

Full live event coverage, whole M7 and comparative performance remain open.

[Machine-readable receipt](../scripts/rust-integration/m7-21514-connect-delivery.json)

Original Dispatcharr and dependency authors and contributors retain full credit.
