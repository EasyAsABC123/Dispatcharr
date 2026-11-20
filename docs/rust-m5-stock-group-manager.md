# Stock React Group Manager cleanup

The original Dispatcharr authors and contributors retain full application and
React credit. [Attribution](../ATTRIBUTION.md)

The packaged browser ran against ordinary Native Main from
`6667998fe8000057e4c08f46fd79a0b11bc751a3`. It used the stock React login,
Channels page and Group Manager. The runner verified the built component hashes
against the tracked React inputs. It reused the paired Original HTTP cleanup
reference at `1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430`. The Original fixture
provided the disposable SQL seed, and its web process stopped before the
Native browser session. The browser workflow ran on Native only.
[Original cleanup reference](../scripts/rust-integration/verified-integration-m5-channel-groups-1.json)

| Browser action | Observed outcome |
| --- | --- |
| Sign in and open Group Manager from Channels | Ordinary login and stock dialog worked with zero uncaught exceptions |
| Cancel cleanup | The browser sent zero cleanup requests |
| Confirm the default cleanup | POST `include_custom=false` returned HTTP 200, reported one deletion and removed the selected loose group from the dialog |
| Confirm cleanup including custom groups | POST `include_custom=true` returned HTTP 200, reported one deletion and removed the selected custom group from the dialog |

All eleven channel rows and the surviving group row matched their complete
before-images. The two intended group rows and one related row were removed.
The browser, baseline startup, Candidate startup, Original web stop and
cleanup all exited zero. Both pipes reached EOF for every owned command.
The browser generation and all four Compose roles retired, with zero owned
containers, networks and volumes left.

The first attempt exited one while opening Group Manager, with unchanged SQL
and exact cleanup. The successful retry opened Channels explicitly after login
and checked toolbar, menu, dialog and group data separately on the same image.
The redacted receipt preserves both attempts and the artifact hashes. The scope
is the stock Group Manager workflow. Full M5 acceptance and performance remain
open. [Browser receipt](../scripts/rust-integration/milestone-batch111/group-manager-browser.json)
