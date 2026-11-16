"""Convert actual bounded socket-read observations into conservative fixed offsets.

Ledger timestamps are monotonic observation times, not inferred kernel arrival.
Never uses post-window bytes as measured useful work.
"""
def offsets(reads, t0, t1):
    if not t0 < t1:
        raise ValueError('FixedPositiveWindowRequired')
    previous_end = 0
    previous_time = float('-inf')
    eligible = []
    for row in reads:
        begin, end = row['begin_monotonic'], row['end_monotonic']
        first, last = row['start_offset'], row['end_offset']
        if not previous_time <= begin <= end or first != previous_end or not 0 < last - first <= 65536:
            raise ValueError('NoncontiguousOrUnboundedReadLedger')
        previous_time, previous_end = end, last
        if begin >= t0 and end <= t1:
            eligible.append((first, last))
    if not eligible:
        raise ValueError('NoWholeReadInFixedWindow')
    if any(a[1] != b[0] for a, b in zip(eligible, eligible[1:])):
        raise ValueError('NoncontiguousWindowReads')
    return eligible[0][0], eligible[-1][1]
