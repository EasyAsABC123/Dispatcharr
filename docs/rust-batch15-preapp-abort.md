# Candidate15 fixture permission abort

Original Dispatcharr and dependency authors retain full credit. React is unchanged.

The [bounded evidence packet](../scripts/rust-milestone-batches/m478/batch15-preapp-abort/README.md)
retains a failed setup run on candidate15. New helper files inherited mode0600
under the preparation umask. The real observer child ran as UID65532 and exited2
before readiness, so application checks did not start. Repeated setup failures
are infrastructure failures, not evidence of nineteen application regressions.

The controller exited-2 after273.115 seconds following Root's SIGINT. That
interrupt landed during Compose teardown, so the normal final cleanup receipt
was not written. Explicit independent cleanup subsequently passed: both exact
containers and recorded host PIDs were absent,217 retained CLI groups had no
current members, project containers/networks/volumes were empty, and cleanup
commands settled with no errors. Earlier false role-generation flags remain
literal and are not rewritten by the later cleanup proof.

The retry preserves the failed source and modes. Its copied tools explicitly use
0444 files and0755 parents, with hash/mode inventory verified before dispatch.
Actual observer readiness must establish UID/GID65532, the same process generation
and an active subscription. Observer death now stops the batch after normal
context cleanup. The retry has its own observations; neither this correction nor
cleanup establishes application parity, a full milestone, or performance gains.
