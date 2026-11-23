# M8 backup schedule after restore on AMD64

An ordinary AMD64 Compose run on exact Rust `7c604a6c` verified that a restored backup schedule runs at its natural due time. The run used signed APIs to schedule a daily UTC backup, create an archive while the schedule was enabled, disable the live schedule and restore that same archive. The restored row was enabled with retention of two archives and a run count of zero.

Redis recorded a natural publication 3.762341 seconds after the scheduled time. The worker completed the job and produced a valid backup ZIP. The matching PostgreSQL row advanced from zero to one run and the observed `last_run_at` matched the due publication. The evaluator joined all three actual jobs for archive creation, restore, and the scheduled backup.

The three PostgreSQL probes returned identical response hashes before restore, after restore and after the job. Each actual probe passed its advisory claim denial check. Internal statement and cache reset telemetry and continuous lock observation remain separate. Ordinary Main exited 0 without OOM. The schedule row persisted across shutdown, a second database stayed unchanged, and all three owned container and process generations and the project network retired. All five owner phases and the independent receipt replay exited 0.

The prior Original versus Rust workflow remains the evidence for the 16 signed backup requests. This new test covers the Rust restored-schedule path. Performance and formal M8 remain open. Original Dispatcharr, React and dependency authors retain full credit. [Machine-readable evidence](../scripts/rust-integration/m8-7c604a6c-amd64-restored-schedule-tick.json)
