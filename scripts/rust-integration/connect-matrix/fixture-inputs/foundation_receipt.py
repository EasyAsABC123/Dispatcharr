"""Keep a known invalid-channel wire difference outside scoped Connect acceptance."""

# Exact earlier Candidate77 and Original observation; no wildcard allowance.
INVALID_CHANNEL_404 = {
    'name': 'status-parity:/proxy/ts/stream/not-a-channel',
    'passed': False,
    'details': {
        'python_status': 404,
        'rust_status': 404,
        'python_body_sha256': 'd870486684062c79c0f177319e065555337713c314ea687952666e74a2c10186',
        'rust_body_sha256': 'c8d3eae160a892e32837db3dcae515e843e5383fef52b8141940c8bcf8b6d59f',
    },
}


def foundation_receipt(checks):
    selected = [row for row in checks if not row['name'].startswith(('connect-reads:', 'connect-logs:'))
                and row['name'] != 'connect-seed-isolation']
    differences = [row for row in selected if not row['passed']]
    unique = len({row['name'] for row in selected}) == len(selected)
    return {
        'passed': bool(selected and unique and not differences),
        'eligible_for_connect_scope': bool(selected and unique and differences in ([], [INVALID_CHANNEL_404])),
        'mismatches': differences,
    }
