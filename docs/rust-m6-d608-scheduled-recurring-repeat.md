# Scheduled recurring maintenance repeat on d608

Stock Original and the exact `d608ff75` Rust package each ran two due
ticks from the same stock hourly recurring-recording row in isolated Docker
Compose projects. The Rust package used Selected Jobs and Beat. The
fixture advanced only the owned row's due input through the ordinary
schedule-change marker and retained its hourly interval.

The first publication returned integer `1`, booked one future UTC
recording with one clocked one-off task, and emitted one recordings refresh
event. The linked artwork child returned `ok`. The second publication had
a distinct task identity and returned integer `0`. The booking and its
one-off row stayed identical. Each scheduler persisted run count `2`.
The journals contained the two roots and one artwork child, and each owned
queue drained before teardown. The future recording stayed scheduled.

Both lane controllers, the Original Beat and worker, and Native Main exited
0. Independent cleanup verified removal of two Original and three Native
containers and their owned host process generations. Both projects had
empty container, network, and volume inventories.
[Redacted paired receipt](../scripts/rust-job-contracts/recurring/verified-scheduled-repeat-d608-1.json)

Broader recurrence calendars, formal M6 scheduler acceptance, default worker
activation, and comparative performance remain open. The prior single-tick
result retains its own package identity.
[Earlier recurring maintenance proof](rust-m6-c2a3-enabled-recurring.md)

The original Dispatcharr, Celery, and django-celery-beat authors and
contributors retain full credit. [Attribution](../ATTRIBUTION.md)
