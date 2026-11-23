# Shared proxy health refresh

With dynamic proxy health enabled, overlapping checks now share one
in-flight refresh. The refresh retains its owner's work if an individual
viewer stops waiting. The original sampling time, typed results, and
writer limits stay in the same scope. Two focused tests verified
overlapping success and failure.

The separate 51912cd0 component fixture used owned PostgreSQL and Redis
to verify the actual shared Engine and Core settings path, the admitted
writer limit, and exact teardown. Default SQL and cache state stayed
unchanged. The formal proxy-health workflow still needs the ordinary
Main timing and fault cases.

Original Dispatcharr and upstream contributors retain full credit.

[Machine-readable component scope](../scripts/rust-integration/m2-51912cd0-core-health-component.json)
