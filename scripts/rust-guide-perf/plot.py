#!/usr/bin/env python3
"""Render the accepted scoped guide ratios from their bound numerical report.
Original Dispatcharr authors retain all application and algorithm credit.
"""
import argparse
import json
import os
from pathlib import Path


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--report', type=Path, default=Path(__file__).with_name('verified-guide-experiment-image28-1.json'))
    parser.add_argument('--output', type=Path, default=Path(__file__).resolve().parents[2]/'docs/images/rust-guide-performance-image28')
    args = parser.parse_args()
    value = json.loads(args.report.read_text())
    if not (value['passed'] and value['full_frozen_guide_matrix_accepted'] and value['completed_pairs'] == 15):
        raise ValueError('Complete accepted 15-pair report required')
    os.environ.setdefault('MPLCONFIGDIR', str(Path(__file__).resolve().parents[2]/'artifacts/rust/matplotlib-cache'))
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    import statistics
    names = ['current-500-shared50', 'xmltv-cold-1', 'xmltv-cold-10', 'xmltv-hot-1', 'xmltv-hot-10']
    # Recompute from all three accepted pair ratios rather than copying labels.
    groups = [[row for row in value['comparisons'] if row['workload'] == name] for name in names]
    if any(len(rows) != 3 or not all(row['passed'] for row in rows) for rows in groups):
        raise ValueError('Missing or failed workload pair')
    cpu = [statistics.median(row['ratios']['cpu_percent_mean'] for row in rows) for rows in groups]
    memory = [statistics.median(row['ratios']['peak_working_memory_bytes'] for row in rows) for rows in groups]
    matplotlib.rcParams.update({'svg.hashsalt':'dispatcharr-image28', 'font.size':10})
    fig, ax = plt.subplots(figsize=(10, 4.8))
    x = list(range(5)); width = .34
    first = ax.bar([n-width/2 for n in x], cpu, width, color='#226eaa', label='Whole-service CPU')
    second = ax.bar([n+width/2 for n in x], memory, width, color='#dc7626', label='Peak sampled working memory')
    ax.axhline(1, color='#555555', linestyle='--', linewidth=1.2)
    ax.text(4.4, 1.025, 'Original backend = 1.0', ha='right', color='#555555')
    for bars in [first, second]:
        for bar in bars:
            height = bar.get_height()
            ax.annotate(f'{height:.3f}', (bar.get_x()+bar.get_width()/2, height), xytext=(0, 4), textcoords='offset points', ha='center', fontsize=9)
    ax.set_xticks(x, ['Current\n500 channels', 'Cold XMLTV\n1 consumer', 'Cold XMLTV\n10 consumers', 'Hot XMLTV\n1 consumer', 'Hot XMLTV\n10 consumers'])
    ax.set_ylim(0, 1.15)
    ax.set_ylabel('Rust / original — lower is better')
    ax.set_title('Stored-guide workload: image28, all 15 paired trials passed', loc='left', weight='bold', pad=16)
    fig.legend([first, second], ['Whole-service CPU', 'Peak sampled working memory'], loc='upper left', bbox_to_anchor=(.085, .825), ncol=2, frameon=False)
    ax.spines[['top', 'right']].set_visible(False)
    ax.yaxis.grid(True, alpha=.16); ax.set_axisbelow(True)
    fig.text(.085, .025, 'Median of 3 paired trials each; backend + PostgreSQL/Redis + origin. Not full-application acceptance.\nOriginal Dispatcharr authors and contributors retain full application and algorithm credit.', fontsize=8, color='#444444')
    fig.subplots_adjust(left=.085, right=.98, top=.85, bottom=.23)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(args.output.with_suffix('.svg'), metadata={'Date':None})
    svg = args.output.with_suffix('.svg')
    svg.write_text('\n'.join(line.rstrip() for line in svg.read_text().splitlines())+'\n')
    fig.savefig(args.output.with_suffix('.png'), dpi=180)
    plt.close(fig)
    print(args.output.with_suffix('.svg'))


if __name__ == '__main__':
    main()
