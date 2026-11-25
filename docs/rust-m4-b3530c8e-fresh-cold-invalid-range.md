# B353 fresh cold invalid VOD range

A new paired fixture verified the exact same known-length Origin and a fresh signed session on each backend. The invalid range reached the Origin in both arms and returned HTTP 416 with zero body bytes. Original returned downstream HTTP 500 with 123 bytes, while Rust returned HTTP 416 with 31 bytes. The independent source trace locates Original's result in the generic upstream-error path and Rust's result in its explicit unsatisfiable-range handling. [Typed evidence](rust-m4-b3530c8e-fresh-cold-invalid-range.json)

The older Original local 416 response used a previously known length. It describes a separate warm state. Exact cleanup passed, and the fresh selected group remains adverse. Original Dispatcharr and all dependency authors retain full credit.
