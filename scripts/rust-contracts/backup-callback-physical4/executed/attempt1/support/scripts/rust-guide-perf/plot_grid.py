#!/usr/bin/env python3
"""Render retained GRID paired ratios. Original Dispatcharr authors retain credit."""
import argparse
import json
from pathlib import Path

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--report', type=Path, required=True)
    parser.add_argument('--output-prefix', type=Path, required=True)
    args = parser.parse_args()
    report = json.loads(args.report.read_text())
    if not report['passed'] or not report['performance_verified']:
        raise ValueError('Accepted complete GRID report required')
    rows = report['workload_medians']
    if [row['users'] for row in rows] != [1, 10] or report['measurement_pairs'] != 6:
        raise ValueError('Unexpected GRID workload scope')
    plt.rcParams.update({'font.family': 'DejaVu Sans', 'svg.hashsalt': 'dispatcharr-grid-image30'})
    figure, axes = plt.subplots(1, 3, figsize=(12, 4.3), sharey=True)
    fields = [('cpu_percent_mean', 'CPU', 'less'),
              ('peak_working_memory_bytes', 'Sampled peak memory', 'less'),
              ('completion_p95_ms', 'Completion p95', 'lower')]
    labels = ['1 admin viewer\n1 request/s', '10 admin viewers\n0.5 requests/s each']
    for axis, (key, title, word) in zip(axes, fields):
        ratios = [100 * row['ratios'][key] for row in rows]
        axis.barh([1, 0], ratios, height=.48, color='#147eaa')
        axis.axvline(100, color='#7f8c95', linestyle='--', linewidth=1)
        for y, value in zip([1, 0], ratios):
            axis.text(value + 2, y, f'{100-value:.1f}% {word}', va='center', fontsize=10)
        axis.set_xlim(0, 112)
        axis.set_xticks([0, 25, 50, 75, 100])
        axis.set_xlabel('Original = 100%')
        axis.set_title(title, fontsize=12, weight='bold')
        axis.spines[['top', 'right', 'left']].set_visible(False)
        axis.grid(axis='x', alpha=.13)
        axis.set_axisbelow(True)
    axes[0].set_yticks([1, 0], labels)
    figure.suptitle('Stored guide grid: Rust backend', fontsize=17, weight='bold', y=.98)
    figure.text(.5, .87, '500 channels · 50 guides · 2,250 programmes · all 6 paired trials passed', ha='center', fontsize=10)
    figure.text(.5, .055, 'Median paired ratios; backend, PostgreSQL/Redis and origin included. Throughput unchanged.', ha='center', fontsize=9)
    figure.text(.5, .012, 'Original Dispatcharr authors and contributors retain all credit. Stored-grid workload only.', ha='center', fontsize=9, color='#46525a')
    figure.subplots_adjust(left=.15, right=.98, bottom=.23, top=.76, wspace=.12)
    args.output_prefix.parent.mkdir(parents=True, exist_ok=True)
    figure.savefig(str(args.output_prefix)+'.png', dpi=160, facecolor='white')
    svg = Path(str(args.output_prefix)+'.svg')
    figure.savefig(svg, metadata={'Date': None}, facecolor='white')
    svg.write_text('\n'.join(line.rstrip() for line in svg.read_text().splitlines())+'\n')
    plt.close(figure)


if __name__ == '__main__':
    main()
