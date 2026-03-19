#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

ROOT = Path(__file__).resolve().parents[1]
MAPPING_PATH = ROOT / 'reference' / 'mapping.json'


@dataclass
class CompareResult:
    name: str
    sequence_ratio: float
    token_jaccard: float
    missing_markers: list[str]
    implementation: Path
    reference: Path
    passed: bool


def strip_comments(sql: str) -> str:
    sql = re.sub(r'/\*.*?\*/', ' ', sql, flags=re.S)
    sql = re.sub(r'--.*', ' ', sql)
    return sql


def normalize(sql: str) -> str:
    sql = strip_comments(sql).lower()
    sql = re.sub(r'\$\{[^}]+\}', ' ', sql)
    sql = re.sub(r"'[^']*'", "'str'", sql)
    sql = re.sub(r'\b\d+(?:\.\d+)?\b', '0', sql)
    sql = re.sub(r'\s+', ' ', sql)
    return sql.strip()


def token_set(sql: str) -> set[str]:
    return set(re.findall(r'[a-z_][a-z0-9_]*', normalize(sql)))


def sequence_ratio(a: str, b: str) -> float:
    # lightweight overlap proxy on normalized strings
    if not a or not b:
        return 0.0
    shorter, longer = (a, b) if len(a) <= len(b) else (b, a)
    hits = sum(1 for token in set(shorter.split()) if token in longer)
    return hits / max(len(set(shorter.split())), 1)


def jaccard(a: set[str], b: set[str]) -> float:
    union = a | b
    if not union:
        return 1.0
    return len(a & b) / len(union)


def load_mapping() -> list[dict]:
    return json.loads(MAPPING_PATH.read_text())['mappings']


def compare_item(item: dict) -> CompareResult:
    implementation = ROOT / item['implementation']
    reference = ROOT / item['reference']
    impl_norm = normalize(implementation.read_text())
    ref_norm = normalize(reference.read_text())

    seq_ratio = sequence_ratio(impl_norm, ref_norm)
    token_ratio = jaccard(token_set(implementation.read_text()), token_set(reference.read_text()))

    required_markers = item.get('required_markers', [])
    missing = [marker for marker in required_markers if marker.lower() not in impl_norm]
    passed = token_ratio >= item.get('min_token_jaccard', 0.10) and seq_ratio >= item.get('min_sequence_ratio', 0.30) and not missing

    return CompareResult(
        name=item['name'],
        sequence_ratio=seq_ratio,
        token_jaccard=token_ratio,
        missing_markers=missing,
        implementation=implementation,
        reference=reference,
        passed=passed,
    )


def render(results: Iterable[CompareResult]) -> str:
    lines: list[str] = []
    for result in results:
        status = 'OK' if result.passed else 'FAIL'
        lines.append(f'[{status}] {result.name}')
        lines.append(f'  implementation: {result.implementation.relative_to(ROOT)}')
        lines.append(f'  reference:      {result.reference.relative_to(ROOT)}')
        lines.append(f'  sequence overlap: {result.sequence_ratio:.3f}')
        lines.append(f'  token jaccard:    {result.token_jaccard:.3f}')
        if result.missing_markers:
            lines.append(f"  missing markers:  {', '.join(result.missing_markers)}")
    return '\n'.join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(description='Compare migrated SQL implementation against reference prototypes.')
    parser.add_argument('--fail-on-low-ratio', action='store_true', help='Return non-zero if any mapping does not satisfy thresholds.')
    args = parser.parse_args()

    results = [compare_item(item) for item in load_mapping()]
    print(render(results))

    if args.fail_on_low_ratio and any(not result.passed for result in results):
        return 1
    return 0


if __name__ == '__main__':
    sys.exit(main())
