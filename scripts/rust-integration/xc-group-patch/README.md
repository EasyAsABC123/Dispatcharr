# Paired XC account management parity

The Original Dispatcharr authors retain full credit for the project, workflows, and behavior tested here.

This workload runs the stock Original and the selected Rust candidate through the normal Main account PATCH and GET routes. Its SQL assertions observe state in a disposable database. The case covers sparse missing-group errors, duplicate first-selection behavior, the React Group Filter preference triple, all three orphan-cleanup values, playlist activation and deactivation, and a playlist editor account rename. It records a projected functional result separately from generated files that contain fixture logs and credentials.

## Run

Start from a full checkout that contains Original commit `1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430` and fixture-support commit `17fae6de0702835c5e94643cccc995df5622d6b1`. Use the existing Candidate build tooling to produce the image and its successful build manifest. Run in a Linux environment with Docker Compose and pass the Docker executable with `stage --docker` if it is outside `PATH`.

```sh
fixture=scripts/rust-integration/xc-group-patch
stage=/path/to/fresh/private-output
python3 -B "$fixture/paired_group_parity.py" stage --output "$stage"
PYTHONPATH="$stage/driver/cohort:$stage/driver" python3 -B "$fixture/test-probe.py"
python3 -B "$fixture/paired_group_parity.py" bind --output "$stage" \
  --manifest "$candidate_manifest" --candidate-revision "$candidate_revision" \
  --mode orphans
python3 -B "$fixture/paired_group_parity.py" run --output "$stage"
```

`references` runs the valid and missing-reference cases. `duplicates` adds duplicate selection and the three account-level preferences. `orphans` adds the three orphan-cleanup transitions. `activity` adds both playlist activation transitions and scoped SQL checks for cron linkage and expiration notifications. `rename` runs the reference checks plus a complete stock-editor account rename, with projected SQL checks for preserved credentials, settings, cron and default profile label. `interval` runs both active and inactive playlist editor interval changes, verifies disable at zero and checks the shared and orphaned Beat schedule rows. The binder verifies the candidate package source archive and requires the relevant implementation before executing a mode.

Staging regenerates the pinned Original tar and fixture support from Git objects. The same generated driver runs both backends. The runner uses the dispatcher lease, preserves the normal startup observations, and checks exact cleanup before starting Native. The Original Beat role stops after startup and before the account requests, while Native runs with its selected scheduler disabled.

`paired-result.json` is the compact functional receipt. Keep the generated driver, archives, fixture directories, dispatcher logs, and raw probe output local. This workload does not measure performance or close the wider milestones.
