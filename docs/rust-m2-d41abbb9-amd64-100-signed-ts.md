# 100 signed TS viewers on d41abbb9 AMD64

The exact ordinary Linux AMD64 Rust image for
`d41abbb90dec665182f2060804e40a4d522b1743` and pinned Original
`1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430` each served 100 concurrent signed
readers of one synthetic MPEG-TS channel in a fresh Docker Compose fixture.
The owner, preflight, physical run and proof each exited 0.
[Machine-readable evidence](../scripts/rust-integration/m2-m3-d41abbb9-amd64-signed-live-formats.json)

Each reader made one request, received HTTP 200, advanced during the common
overlap and reached the capture boundary without an unexpected EOF. Every
2 MiB prefix passed independent FFmpeg audio and video decode. Before and
after the overlap in each arm, the origin had one active connection and the
primary held one provider slot. The cumulative origin counter rose by exactly
one per arm. After all viewers disconnected, each arm reached zero viewers,
zero active upstream connections and no remaining provider assignment.

The independent audit matched all 200 media hashes and decoder transcript
hashes to the actual receipt and checked each capture's decoded video frame
count against the 25-frame floor. Original counts ranged from 72 to 98 and
Native counts were 124 for every capture. Those counts describe the captured
prefixes only. All five owned Compose roles had exact container and process
generation retirement, and the project inventories were empty.

The fixture verified functional 100-viewer fanout and shared upstream use.
Formal M2 performance and the other configured scenarios remain open. The
original Dispatcharr, Django, FFmpeg and dependency authors retain full credit.
