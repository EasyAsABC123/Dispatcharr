# Original scheduled aggregate XMLTV refresh

One disposable Compose run exercised Original Dispatcharr revision `1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430` with stock image `sha256:34f9011514edd31ef0ff111b5f256d2e37614fcb08a6ee1ed9eccec9251a856f`. Original Dispatcharr, Celery and Django authors retain full credit.

The owned custom Beat row advanced from zero to one execution. The stock prefork worker consumed `apps.epg.tasks.refresh_all_epg_data` from the ordinary Celery queue. The published arguments were empty, the task identified itself as the root, and parent and both execution limits were null. There were no standalone individual-refresh publications.

Two active XMLTV sources received identical payloads in the observed order alpha then beta. Each ended with status success, one programme and a populated index. The inactive source and active dummy stayed idle with zero programmes and no index. The observed query order is specific to this disposable fixture.

The same natural broker journal recorded Index(alpha), Series(), Index(beta), Series() in that order. The aggregate was both root and parent of every child. The root completed first with the exact string `EPG data refreshed.` and four ordered Celery tuples of the form `[[child_id, null], null]`. The children completed in publication order. Index returned null and Series returned `{"scheduled":0,"details":[]}` for this fixture. Their own result trails were empty. Zero work remained ready or reserved.

The controller and lane exited 0. The independent cleanup receipt verified both exact containers and their original host process generations absent, with empty project container, network and volume inventories. Two earlier physical attempts remain separate failures with exact cleanup. The first stopped during seed selection because the cloned schema contained an inherited active source. The second found one queued task immediately after seeding, before the stock worker or Beat started. A later fixture revision used quiet channel-to-guide assignment and retained the empty-broker assertions.

The redacted projection SHA-256 is `e32c86d0d21d7b4619c10d0b9c6ddd73dd9d693574a73b751ae7130178e9f7e0`. Exact controller receipt SHA-256 is `f8ce0b9032589880368ea450d50e711f1960fbce0371db0376974355fba01835`. This run qualifies Original behavior for the two owned ordinary XMLTV sources. Native aggregate behavior, other source classes, broader M6 and performance remain open.
