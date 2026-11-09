# Native M3U output prototype

All original application credit belongs to Dispatcharr authors/contributors.
Source: `apps/output/views.py` and its channel/provider helpers. Retained AGPL
license and attribution apply. No M7 parity or performance gain is claimed.

Actual original HTTP characterization covers38 anonymous/XC M3U cases, with
credential-bearing request URLs/export bodies omitted from persisted evidence.
See `scripts/rust-output-contracts/README.md`. The renderer here is an unwired,
uncompiled draft pending actual SQL/auth/router and exact body comparison.
Provider transforms, bounded cache/bodies, actual POST CSRF, source system events,
Docker/decoder/resource acceptance remain open. XMLTV, Xtream and HDHR follow.
