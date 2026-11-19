# Retained Original clocked recording and artwork tasks

Original Dispatcharr, Celery, django-celery-beat, FFmpeg and dependency
authors retain all application and algorithm credit.

The [Source3 capture](../../rust-job-contracts/clocked-recording-source/README.md)
records one natural future-clocked recording. Original prefetch_recording_artwork
ran on celery and returned ok. Original run_recording ran on dvr and
returned null. Both own-root tasks have matching raw frame, worker prerun and
postrun, and Redis backend identities. The recording row retained its
PeriodicTask name, owned marker and artwork through completed remux.
The controller and host exited 0 with exact container, network, volume and process
cleanup. Two static scanner publications remained queued and unconsumed.
Source1 and Source2 retain their prior failed outcomes.

Run `python3 -B scripts/rust-contracts/retained_clocked_recording_contracts.py`
to reproduce the typed assignments from the published files.

The binding covers only the captured future-clocked scenario. Native coverage,
broader lifecycle behavior, milestone acceptance and performance remain open.
