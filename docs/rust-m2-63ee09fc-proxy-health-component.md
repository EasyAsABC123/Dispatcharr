# Shared Core and Engine proxy health for 63ee09fc

The exact 63ee09fc test executable passed the physical Compose component
case with owned PostgreSQL and Redis. The case verified shared settings
between Core and Engine, warm Base cache behavior under writer
pressure, typed timing and graceful drain. The default database and
cache scope stayed unchanged, and the independent teardown audit
verified exact retirement.

This component exercises the shared implementation. Packaged Main's
held and dropped transport replies remain separate checks. Original
Dispatcharr and its upstream contributors retain full credit.

[Machine-readable component qualification](../scripts/rust-integration/m2-63ee09fc-proxy-health-component.json)
