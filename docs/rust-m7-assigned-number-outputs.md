# Assigned channel numbers in M3U and HDHomeRun

Pinned Original and packaged Rust `08661ba2` produced byte-identical
M3U playlists and HDHomeRun lineups before and after three ordinary Assign
Channel Numbers submissions. The four phases checked eight protocol GETs
per backend through an isolated Compose network. All five owned channels
appeared in effective number order, with decimal formatting, the managed
display name and the expected internal playback references.

Each assignment retained the existing SQL and ordered channel API refetch
checks. The protocol reads left the channel and profile membership rows
unchanged. Both applications served all eight outputs with HTTP 200 and
the matching media type, download disposition where present, body length
and SHA-256. All four owned container and process generations retired.
[Redacted Compose receipt](../scripts/rust-output-contracts/verified-assigned-number-outputs-08661.json)

The first controller stopped at output admission before Docker. The
first physical attempt stopped during Original fixture creation and
retired both started roles. The fixture had used default profile creation,
whose signal populated memberships before the fixture attempted to insert
the same rows. The successful fresh pair used the Original
serializer's `start_empty` branch and added exactly the five owned
memberships. Results apply to `08661ba2` and these two output surfaces.
Full M7 and performance verification remain open.

Original Dispatcharr contributors retain full application and behavior
credit. [Dispatcharr upstream](https://github.com/Dispatcharr/Dispatcharr)
