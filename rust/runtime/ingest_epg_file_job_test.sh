#!/bin/sh
# Disposable component runner; all original application credit is retained.
set -eu
case "${EPG_TEST_SUITE:-}" in
  source_sql|policy_cache_sql|reporter_sql|channels_sql|file_job_sql) exec "/test-bin/$EPG_TEST_SUITE" --ignored --nocapture --test-threads=1 ;;
  *) exit 64 ;;
esac
