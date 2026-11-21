"""Summarize mutmut 3 results and enforce the M1 gate (killed / (killed + survived) >= 80%)."""

import collections
import pathlib
import sys

work = pathlib.Path(sys.argv[1])
threshold = float(sys.argv[2]) if len(sys.argv) > 2 else 0.80
counts = collections.Counter()
per_module = collections.defaultdict(collections.Counter)
survivors = []
for line in (work / "results.txt").read_text().splitlines():
    if ":" not in line:
        continue
    name, status = (part.strip() for part in line.rsplit(":", 1))
    counts[status] += 1
    module = name.split(".x", 1)[0].split("__mutmut")[0]
    per_module[module][status] += 1
    if status == "survived":
        survivors.append(name)
killed, survived = counts["killed"], counts["survived"]
score = killed / max(1, killed + survived)
for module, c in sorted(per_module.items()):
    k, s = c["killed"], c["survived"]
    print(f"{module:55s} killed={k:4d} survived={s:4d} score={k / max(1, k + s):.0%}")
print(f"TOTAL killed={killed} survived={survived} other={dict(counts)} score={score:.1%}")
(work / "survivors.txt").write_text("\n".join(survivors))
sys.exit(0 if score >= threshold else 1)
