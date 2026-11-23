# ARM64 signed same-client VOD reconnect

Original `1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430` and the signed ordinary ARM64 package for `381a6a157d1f860527906794bb8c63859fcc8176` passed the private Lappy Compose pair. Each application issued an HTTP 301 playback URL. The same signed client then completed two HTTP 200 reads of that URL. Each response contained the complete 4,299,461-byte MP4, SHA-256 `f4a7cbe4e50eaf6cd1578933b8eb8701eb83b384182d6af1f488461e5586dd2f`, and passed strict full H.264 video and AAC audio decoding. Each arm independently regenerated and verified those exact bytes before starting the application.

Both arms recorded Redis request counts 1 and 2 on the same server-issued session, retained its creation marker, and reached idle after the first EOF. The provider slot was released after each read and the session retired after the second EOF. The independent origin census recorded two complete requests and 8,598,922 bytes per arm. Temporary payloads stayed within the private test scopes.

Both applications and probes exited `0`. The outer owner, preflight, run, proof and separate receipt audit all exited `0`. The independent audit verified typed absence of all 14 containers, both network IDs and names, and four named volumes. Both project inventories were empty. Raw pair SHA-256 `11d7e316682fd21b2f590270b1bd31b011fd12de7a3c8f014c919ec0859fcb20`.

The original cross-architecture input check remains recorded as a failed attempt. The existing AMD64 reference was 4,299,455 bytes. The independently observed ARM64 file was six bytes larger and had a different digest. The retry used the unchanged generator and 262,144-byte-per-second configured pace under a separate exact ARM64 input pin. It proved byte identity across the two ARM64 arms. Broader VOD seek and range cases, process recovery, performance, and full M4 acceptance remain separate gates.

Application and React credit belongs to the original Dispatcharr authors and contributors.
