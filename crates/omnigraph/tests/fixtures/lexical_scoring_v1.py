"""Regenerate lexical_scoring_v1.json after an intentional RFC 0048 change.

Uses 80-digit Decimal arithmetic over already-analyzed fixture terms. This
is a numerical design oracle, not a tokenizer or retrieval benchmark. Cargo
does not run this generator: the checked-in expectations require review.
"""
from collections import Counter
from decimal import Decimal, localcontext
import json
from pathlib import Path

def distance(a, b):
    grid = [[0] * (len(b) + 1) for _ in range(len(a) + 1)]
    for i in range(len(a) + 1):
        grid[i][0] = i
    for j in range(len(b) + 1):
        grid[0][j] = j
    for i, ca in enumerate(a, 1):
        for j, cb in enumerate(b, 1):
            grid[i][j] = min(grid[i-1][j] + 1, grid[i][j-1] + 1,
                             grid[i-1][j-1] + (ca != cb))
    return grid[-1][-1]

cases = [
    ("rare_variant", ["beta", "beta", "beta", "beto", "gamma"], ["beta"], 1, False),
    ("zero_edits", ["beta", "beta", "beta", "beto", "gamma"], ["beta"], 0, False),
    ("zero_edits_multi_term", ["beta gamma gamma", "beta", "gamma", "zeta"], ["gamma", "beta"], 0, False),
    ("alternatives_do_not_add", ["beta", "beta beto", "beta gamma", "beto", "beta beta", "gamma"], ["beta"], 1, False),
    ("one_term_satisfies_two_groups", ["beto", "beta", "beta beto", "gamma"], ["beta", "beto"], 1, True),
    ("all_counts_each_family_before_query_filter", ["beta gamma", "beto gamma", "beta", "gamma", "gamma beta beta", "other"], ["beta", "gamma"], 1, True),
    ("any_uses_the_same_statistics", ["beta gamma", "beto gamma", "beta", "gamma", "gamma beta beta", "other"], ["beta", "gamma"], 1, False),
    ("null_and_token_empty_are_outside_corpus", [None, "", "beta", "beto", "beta gamma"], ["beta"], 1, False),
    ("two_edits_and_transposition", ["abcd", "abce", "abef", "ab", "acbd", "zxyz"], ["abcd"], 2, False),
    ("unicode_scalar_edit", ["café", "cafe", "other"], ["cafe"], 1, False),
    ("empty_corpus", [], ["beta"], 1, False),
    ("no_matching_family", [None, "", "gamma"], ["beta"], 1, False),
]

out = []
with localcontext() as ctx:
    ctx.prec = 80
    D = Decimal
    for name, docs, query, edits, all_terms in cases:
        terms = sorted(set(query))
        counts = [Counter((doc or "").split()) for doc in docs]
        n = sum(bool(c) for c in counts)
        total_length = sum(sum(c.values()) for c in counts)
        scores = [None] * len(docs)
        if n:
            avg = D(total_length) / n
            dfs = {q: sum(any(distance(q, t) <= edits for t in c) for c in counts) for q in terms}
            for i, c in enumerate(counts):
                contributions = []
                for q in terms:
                    weights = []
                    for t, freq in c.items():
                        edit = distance(q, t)
                        if edit <= edits:
                            norm = D("1.2") * (D("0.25") + D("0.75") * sum(c.values()) / avg)
                            weights.append((D(2) ** -edit) * D("2.2") * freq / (D(freq) + norm))
                    if not weights:
                        contributions.append(D(0))
                    else:
                        idf = (1 + (D(n - dfs[q]) + D("0.5")) / (D(dfs[q]) + D("0.5"))).ln()
                        contributions.append(idf * max(weights))
                matched = all(v > 0 for v in contributions) if all_terms else any(v > 0 for v in contributions)
                if matched:
                    scores[i] = sum(contributions)
        rank = sorted((i for i, s in enumerate(scores) if s is not None), key=lambda i: (-scores[i], i))
        out.append(dict(name=name, docs=docs, query=query, edits=edits, all_terms=all_terms,
                        expected_scores=[str(s) if s is not None else None for s in scores],
                        expected_order=rank))
        print(name, rank, [round(float(s), 9) if s is not None else None for s in scores])

path = Path(__file__).with_suffix(".json")
path.write_text(json.dumps(out, indent=2, ensure_ascii=False) + "\n")
