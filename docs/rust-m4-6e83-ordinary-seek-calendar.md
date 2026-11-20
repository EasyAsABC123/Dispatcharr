# Ordinary VOD numeric and catch-up Range requests on 6e83

Pinned Original and packaged Rust Main each passed five ordinary
authenticated numeric seeks and five calendar catch-up seeks. All
20 requests returned HTTP 206 with the expected complete range.
Every request caused one expected upstream request.

Both isolated Compose projects exited 0. Independent cleanup verified
all eight owned role generations and empty project inventories. Full M4,
session compatibility, and performance remain open.

The original Dispatcharr authors retain full application and protocol credit.
[Redacted receipt](../scripts/rust-integration/m4-ordinary-seek-calendar-6e83d3d.json)
