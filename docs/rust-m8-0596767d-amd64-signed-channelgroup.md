# M8 signed ChannelGroup changelist on AMD64

The pinned Original AIO returned HTTP 200 for an active staff user's signed, view-only `GET /admin/dispatcharr_channels/channelgroup/` through both direct Django ingress and the stock public ingress. Ordinary direct login issued a Django database session, rotated CSRF, and bound the session to the owned actor with the exact view permission. The changelist displayed the owned row and Django static-media references, with no actions. Typed snapshots of the owned user, session, group, direct permissions, and admin log count were identical across both reads. Ordinary logout returned HTTP 200, expired the cookie, and removed the owned session. Anonymous public access redirected to the precise Django login URL, whose public ingress redirected to the React login.

The exact Rust `0596767d` package returned HTTP 404 with `{"detail":"Not found."}` for the signed list request. Its disposable foundation issued the Django session and verified its owned actor binding before the request. Native emitted no login location, cookie, or Bearer challenge for that request. The separate public login request returned the existing HTTP 301 React redirect. This documents the changelist ingress gap and the two public login boundaries. The Rust run exercised the fixture-issued session input at ingress only. Rust session issuance, authorization, and rendering remain open.

Both owned Compose projects retired exactly. Stock's one owned runtime generation was absent after cleanup. Native's four owned roles and process identities were absent, and both project inventories were empty. The stock, Native, reducer, and wrapper exits were all 0 for this diagnostic. Its literal ModelAdmin parity result is false. No formal M8 gate closed. The Native ingress source is byte-identical on application `d41abbb9` and its docs-only publication `896b4689`. Those two revisions have source-level confirmation for this routing branch.

Original Dispatcharr, Django, nginx, and their dependencies retain full author credit.

[Machine-readable evidence](../scripts/rust-integration/m8-0596767d-amd64-signed-channelgroup.json)
