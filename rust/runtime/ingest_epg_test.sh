#!/bin/sh
# Disposable component runner; all original application credit is retained.
set -eu
case "${EPG_TEST_SUITE:-}" in
  source_sql|policy_cache_sql) exec "/test-bin/$EPG_TEST_SUITE" --ignored --nocapture ;;
  *) exit 64 ;;
esac
