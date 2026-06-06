#!/usr/bin/env python3
"""Report-only JMH regression comparator (design §11.4 / §15 risk #9).

Compares a current JMH results.json against a committed baseline.json and prints a per-benchmark
delta, flagging anything past the ±10% threshold. This is the *aspirational* >10% regression gate:
it is intentionally advisory (exit 0 always) in v1 because JMH numbers are noisy on shared CI
hardware and a flaky perf gate is worse than none (risk #9 "targets are PROJECTIONS until
measured", risk #20 CI flakiness). Flip `--strict` on once a dedicated low-variance runner exists.

Usage:  python3 compare.py baseline.json current.json [--threshold 0.10] [--strict]

JMH score semantics:
  * mode "thrpt"  -> ops/time, HIGHER is better; a regression is a score DROP.
  * mode "ss"/"avgt"/"sample" -> time/op, LOWER is better; a regression is a score RISE.
"""
import json
import sys


def load(path):
    with open(path) as f:
        data = json.load(f)
    out = {}
    for rec in data:
        name = rec["benchmark"].rsplit(".", 1)[-1]
        out[name] = (rec["primaryMetric"]["score"], rec.get("mode", "thrpt"), rec["primaryMetric"]["scoreUnit"])
    return out


def main(argv):
    if len(argv) < 3:
        print(__doc__)
        return 0
    baseline_path, current_path = argv[1], argv[2]
    threshold = 0.10
    strict = False
    for a in argv[3:]:
        if a == "--strict":
            strict = True
        elif a.startswith("--threshold"):
            threshold = float(a.split("=", 1)[1]) if "=" in a else 0.10

    baseline = load(baseline_path)
    current = load(current_path)
    regressions = 0
    print(f"{'benchmark':<36} {'baseline':>14} {'current':>14} {'delta':>9}  status")
    print("-" * 84)
    for name in sorted(current):
        cur_score, mode, unit = current[name]
        if name not in baseline:
            print(f"{name:<36} {'(new)':>14} {cur_score:>14.1f} {'--':>9}  new")
            continue
        base_score = baseline[name][0]
        lower_is_better = mode in ("ss", "avgt", "sample")
        # Positive pct == improvement, negative == regression, regardless of direction.
        if lower_is_better:
            pct = (base_score - cur_score) / base_score
        else:
            pct = (cur_score - base_score) / base_score
        status = "ok"
        if pct < -threshold:
            status = "REGRESSION"
            regressions += 1
        elif pct > threshold:
            status = "improved"
        print(f"{name:<36} {base_score:>14.1f} {cur_score:>14.1f} {pct * 100:>8.1f}%  {status}  ({unit})")

    print("-" * 84)
    if regressions:
        print(f"{regressions} benchmark(s) regressed > {threshold * 100:.0f}% vs baseline.")
        return 1 if strict else 0
    print("No regressions past threshold.")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
