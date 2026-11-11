#!/bin/sh
# Keep the owned core cgroup alive after its real worker exits. Both backends
# run in the same pinned Original image; native never launches Python here.
set +e
case "$M1_CORE_BACKEND" in
 source)
   /dispatcharrpy/bin/python /core-tools/source_core.py
   status=$?
   ;;
 native)
   /evidence/native-core-peer "$M1_CORE_TEST" --exact --ignored --nocapture --test-threads=1
   status=$?
   ;;
 *) status=64 ;;
esac
printf '{"worker_exit_code":%s,"wrapper_retained":true}\n' "$status" > "$M1_CORE_OUTPUT.exit.json"
exec sleep infinity
