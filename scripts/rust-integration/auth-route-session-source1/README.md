Actual Source auth route/session observations

Original Dispatcharr, Django, DRF and SimpleJWT authors retain full credit.

Root55191 completed the capture controller with exit0 in141.362986743s;45 requests were retained and independently checked offline. This is not all-three-role acceptance: streamer login returned real429 and its five dependent calls were NOT_RUN. A future fresh-context streamer login plus those five calls is six new requests, not a rewrite of the old429.

The real admin CSRF/login/session chain and JWT+cookie logout were observed; the session row is removed, session cookie expired, later admin GET redirects, and the original JWT still authorizes its own user. Standard JWT login/refresh/logout succeeds. Exact three-route method/OPTIONS/error responses and429 Retry-After bodies are retained as selected safe facts. No general Django admin model/form support is implied.

The independently verified raw selected SQL chain covers owned users, all sessions, all SystemEvents and two sequences. Credential fields remain stable except actual last_login updates. Raw SQL/HTTP containing passwords, JWTs, cookies and CSRF stay private;180 exact hashes/lengths bind them. Public facts include no usable credentials. Login-throttle Redis history and notification inbox were not directly captured and are not silently declared equivalent. JWT claims/type/lifetimes are checked and issued tokens were used by real protected GETs; no offline JWT signature-verification claim is made.

Source code uses one3/minute trusted-client-IP login bucket shared by JWT and real admin session login. AdminJWT, adminsession and standardJWT consume its three slots; the streamer and wrong-password alias429s are expected throttling, not invalid-credential401 evidence. Root cleanup proves three exact CIDs, two original host generations and the project absent. No Native, performance or full-milestone acceptance is claimed.
