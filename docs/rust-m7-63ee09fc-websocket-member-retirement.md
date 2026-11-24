# WebSocket member retirement on 63ee09fc

A paired ordinary Compose comparison verified nine membership
transitions on both backends. A normal close and an interrupted
transport each removed the owned subscription. A separate admin
kept receiving real signed status events while the Standard
subscriber remained isolated from admin events. Reconnect acquired
a fresh membership, and the final group disappeared.

Both arms passed exact teardown. This scope covers the ordinary
status producer and selected member lifecycle. Further slow-reader,
fault and full realtime checks remain open.

Original Dispatcharr and its upstream contributors retain full credit.
[Machine-readable membership qualification](../scripts/rust-integration/m7-63ee09fc-websocket-member-retirement.json)
