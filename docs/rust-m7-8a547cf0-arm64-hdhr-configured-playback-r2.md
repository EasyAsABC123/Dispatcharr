# ARM64 HDHomeRun configured playback

Original `1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430` and the signed ordinary Rust package for `8a547cf00cd33a94fe4ccb982272be1861d52e73` passed a paired ARM64 Compose test. The Rust build manifest digest was `ca566d42a506aa5ba5ee6455558dc8cab0cee05af2d2308ab19bbda64bc26cf1`. The owner preflight, run, proof and independent receipt audit each exited `0`.

Each arm returned matching discovery, followed lineup and scan status in two cases. The bare active output profile listed guides 2.5 and 12 with the explicit profile parameter. The named inactive output profile listed guide 2.5 and advertised its pass-through stream. The configured client followed that returned URL and received HTTP 200 `video/mp2t` in both arms. Each arm strictly decoded H.264 video and AAC audio, reported 74 frames and produced an identical 7,000-packet, 1,316,000-byte MPEG-TS sample. The shared SHA-256 was `8b8d32a8a93c39e130ee13c4c39b9125cfcddf5397a88f8fe937b0b2cf7f1c1c`. Each arm opened one primary upstream connection and retired it. The fallback origin and eight database table projections were unchanged in each arm.

Both applications stopped with exit `0`. An independent audit verified typed absence of all 14 owned containers, both network IDs and names, and four volumes, with empty project inventories. The raw pair digest was `f6ff9b905f3b2c5cc85fcf5fe60555af9240520eabb6b805ee25266b830bbc4f`. The bounded samples were processed in memory, and only sanitized evidence left Lappy.

The retry used the stock Django WSGI application via the existing rootless uWSGI/gevent playback configuration. The earlier direct-Daphne attempt remains a separate failed receipt. This result covers configured HTTP playback in a private synthetic network. Physical hardware, UDP discovery, active-profile transcoding, and full M7 acceptance remain open.

Application and React credit belongs to the original Dispatcharr authors and contributors.
