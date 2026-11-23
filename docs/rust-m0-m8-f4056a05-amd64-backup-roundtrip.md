# AMD64 backup inventory and download on f4056a05

The ordinary Linux AMD64 package at `f4056a05` matched Original on status and
media type for eight GETs across three backup routes. The Administrator
listed one owned archive, requested its download token, used that issued
token in a browser-style download request, downloaded the same file with
signed admin authentication, and repeated inventory. The signed Standard
account received 403 for inventory and token issuance and 401 for direct
download. The three denial bodies and all JSON response lengths matched.

Both download paths in each arm returned the exact locally seeded ZIP
bytes, the correct declared length and the attachment header. Each
independently generated stock v2 ZIP contained one PostgreSQL custom dump
and one metadata file. Original served 39,065 bytes and Rust served
39,053 bytes. Each arm preserved its selected PostgreSQL rows, archive
inventory and Redis broker state. Both isolated Compose projects retired
their exact scoped containers and captured process generations, with no
remaining owned containers, networks or volumes.

The selected headers differed on all eight GETs. Original sent
`Allow: OPTIONS, GET` and `Vary: Accept, origin`. Rust omitted `Allow` and
sent `Vary: origin`. The fixture exercises ordinary backup reads with
background workers disabled. Whole M0 and M8 acceptance, archive execution,
restore, background work and performance remain outside the result.
[Scoped physical evidence](../scripts/rust-integration/m0-m8-f4056a05-amd64-backup-roundtrip.json)

Original Dispatcharr authors and contributors retain full application and
protocol credit.
