# Candidate5 versioned Core99 evidence

Original Dispatcharr authors retain all application and algorithm credit. This
namespace preserves the historical `../core99` executed closure unchanged.
`driver/core51_cohort.py` is the changed candidate5 collector; all its imports
are standard library. It adds live Main process/hostname/thread observation.
The unchanged controller/collectors remain in the historical namespace and the
full actual executed candidate5 snapshot is retained at
`artifacts/rust/m478-core51-main-batch5-prep-1/controller`. This directory is a
versioned changed-file handoff, not a claim of a standalone live dispatcher.

Read-only reruns (no application, Docker, SQL or Source API):

```sh
python -B qualify.py --output /path/to/artifacts/rust/milestone-batch5-Core99-1
python -B review/qualify_metadata.py /path/to/artifacts/rust/milestone-batch5-Core99-1
```

The first entry point references shared immutable historical collectors through
`../core99/driver`, and its own review helpers through `review`. Results in
`evidence` retain their actual candidate5 scopes and gaps. No historical driver
or evidence byte is replaced.
