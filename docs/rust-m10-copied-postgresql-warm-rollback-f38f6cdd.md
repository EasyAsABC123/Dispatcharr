# F38 AMD64 warm rollback with copied PostgreSQL

The exact ordinary F38 AMD64 package passed a warm rollback on the
isolated Compose fixture. The front door switched from Original to
Native and back. The same signed session retained media access and saw
the acknowledged channel rename. Only the channel name and its
`updated_at` changed. The Native writer retired before Original
reconciled that row. The original web, PostgreSQL and Redis process
generations survived the cutback, and unrelated rows and schema matched.

The fixture cutback took **7.862 seconds** in this single rehearsal.
Both image identities matched before and after, the dedicated engine
inventories were empty at both boundaries, and all seven queue, owner
and proof exits were zero.

| Independently retired scope | Containers | Observed process generations |
| --- | ---: | ---: |
| Donor | 2 | 2 |
| Original control | 2 | 2 |
| Warm trial | 5 | 3 |

The independent file-only replay reproduced the original owner proof
byte for byte. It binds application `f38f6cdd09ad214d3d3ff18b0a46c286637fc9d1`,
published revision `7927b2d4765f82b650d958fd15d0b70200a6b72c` and
AMD64 build manifest
`dc526bb02476e8610b8f79ba018242fcd34bbaa1c19988ee4b7f9e440a761387`.
The owner proof SHA-256 is
`04e3f2457ac03526564ce94e303f006960e7670cc91cece62025ac3c0d512179`.

The same F38 application separately passed the ARM64 cold rollback.
Together these results cover the scoped AMD64 warm and ARM64 cold drills.
Full M10 acceptance, the performance gate and production release retain
their own requirements. The original Dispatcharr and dependency authors
retain full authorship and license credit.
