#!/usr/bin/env python3
"""Plot a complete accepted VOD API experiment. Original authors retain credit."""
import argparse
import json
from pathlib import Path

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--report', required=True, type=Path)
    parser.add_argument('--output-prefix', required=True, type=Path)
    args = parser.parse_args()
    report = json.loads(args.report.read_text())
    if not report['passed'] or not report['performance_verified'] or report['pairs'] != 6:
        raise ValueError('Complete accepted six-pair VOD report required')
    rows = [report['medians'][f'ordinary-lists-{readers}'] for readers in [1, 10]]
    plt.rcParams.update({'font.family': 'DejaVu Sans', 'svg.hashsalt': 'dispatcharr-vod-api-image31'})
    figure, axes = plt.subplots(1, 3, figsize=(12, 4.3), sharey=True)
    fields = [('cpu_percent_mean', 'CPU', 'less'),
              ('peak_working_memory_bytes', 'Sampled peak memory', 'less'),
              ('completion_p95_ms', 'Completion p95', 'lower')]
    for axis, (key, title, word) in zip(axes, fields):
        values = [100 * row['paired_ratios'][key] for row in rows]
        axis.barh([1, 0], values, height=.48, color='#147eaa')
        axis.axvline(100, color='#7f8c95', linestyle='--', linewidth=1)
        for y, value in zip([1, 0], values):
            axis.text(value + 2, y, f'{100-value:.1f}% {word}', va='center', fontsize=10)
        axis.set_xlim(0, 112)
        axis.set_xticks([0, 25, 50, 75, 100])
        axis.set_xlabel('Original = 100%')
        axis.set_title(title, fontsize=12, weight='bold')
        axis.spines[['top', 'right', 'left']].set_visible(False)
        axis.grid(axis='x', alpha=.13)
        axis.set_axisbelow(True)
    axes[0].set_yticks([1, 0], ['1 virtual reader\n1 request/s', '10 virtual readers\n1 request/s each'])
    figure.suptitle('Ordinary VOD list API: Rust backend', fontsize=17, weight='bold', y=.98)
    figure.text(.5, .87, '5,000 movies · 5,000 series · 50,000 episodes · all 6 paired trials passed', ha='center', fontsize=10)
    figure.text(.5, .055, 'Median paired ratios; backend, PostgreSQL/Redis and idle origin included. Throughput unchanged.', ha='center', fontsize=9)
    figure.text(.5, .012, 'Original Dispatcharr authors and contributors retain all credit. List API workload only.', ha='center', fontsize=9, color='#46525a')
    figure.subplots_adjust(left=.15, right=.98, bottom=.23, top=.76, wspace=.12)
    args.output_prefix.parent.mkdir(parents=True, exist_ok=True)
    figure.savefig(str(args.output_prefix)+'.png', dpi=160, facecolor='white')
    svg = Path(str(args.output_prefix)+'.svg')
    figure.savefig(svg, metadata={'Date': None}, facecolor='white')
    svg.write_text('\n'.join(line.rstrip() for line in svg.read_text().splitlines())+'\n')
    plt.close(figure)


if __name__ == '__main__':
    main()
