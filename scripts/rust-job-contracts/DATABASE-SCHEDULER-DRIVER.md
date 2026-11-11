# Owned known-interval timer

Original Dispatcharr/Celery/django-celery-beat authors retain credit and licenses. This component follows the pinned Original lifecycle and does not enable application/default intake.

`verified-source-native-scheduler-driver-3.json` binds a 70-check real-timer comparison (50 Original, 20 native) and 47 setup/resource checks with four actual physical tests. Actual Original Beat and the native owned timer each published three distinct tasks at elapsed one-second intervals for inactive refresh and no-rules Series. Real Original workers consumed the directly published tasks, actual AsyncResult values matched, and graceful shutdown persisted all three runs. All owned projects were removed and default fixture databases remained unchanged.

The driver starts only after explicit preparation. It stops on unsupported entries/options rather than skipping or acknowledging them. Stop requests let the admitted tick finish. One absolute shutdown deadline covers that tick, physical replies and final SQL sync. A timed-out or canceled shutdown wait retains its join handle for a later drain. An actual PostgreSQL table-lock test verified retained caller/admission ownership, an incomplete short deadline, cancellation of the waiting shutdown future, then complete physical drain without publication after stop.

The first two driver physical attempts failed before the shutdown assertions: the fixture bulk-disabled the already cached default rows without changing the Beat sentinel. This correctly left the cache stale and the driver returned Unsupported; bounded `pg_stat_activity` showed its marker read and idle connection. Attempt three established the baseline marker and sent the real sentinel before holding the table. Runtime and time budgets were unchanged. The failures and diagnostic observations remain retained.

The real-timer and final physical snapshots differ only in the ignored physical-test module; the public summary checks that all runtime inputs match. The final selected tree passed strict Clippy and 40 default units, with 30 external tests ignored.

Still open: native consumer end-to-end compatibility, application startup configuration, multiple enabled entries/heap ties, arbitrary default task families/options, calendar/timezone/DST behavior. The earlier single-interval and process-crash proofs keep their separate identities. The actual Original before-sync publication duplication is preserved, not normalized into an exactly-once guarantee. No whole-M6 or performance claim follows from this component.
