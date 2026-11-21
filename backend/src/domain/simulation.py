"""Daily overtime probability simulation for pending subtitle review work.

The simulation pool is the mean estimation error (estimate minus actual) of
each holdout day. A batch draws one error per pending day and iteration, so
every ticket on the same day shares that error inside one iteration. A day's
simulated workload is therefore ``estimate_total - job_count * error``.
"""

from math import ceil
from statistics import fmean


def batch_plan(iterations, batch_size):
    """Split the requested sample count into deterministic batch sizes."""
    if iterations < 1:
        raise ValueError("模拟次数必须为正")
    if batch_size < 1:
        raise ValueError("每批样本数必须为正")
    if batch_size > iterations:
        raise ValueError("每批样本数不能大于模拟次数")
    if (iterations + batch_size - 1) // batch_size > 400:
        raise ValueError("批次数最多 400 个，请调大每批样本数")
    return [min(batch_size, iterations - start) for start in range(0, iterations, batch_size)]


def sample_batch(inputs, size, seed):
    """Draw one shared estimation error per pending day for every iteration.

    The returned matrix is indexed by pending day and then by iteration. The
    batch is reproducible from its seed, so a retried batch replaces the same
    row instead of adding a second copy of the samples.
    """
    import random

    pool = [item["mean_error"] for item in inputs["day_errors"]]
    if not pool:
        raise ValueError("缺少留出日的平均估算误差，无法抽样")
    days = inputs["pending_days"]
    if not days:
        raise ValueError("没有待审工单，无法抽样")
    if size < 1:
        raise ValueError("每批样本数必须为正")
    generator = random.Random(seed)
    return [[generator.choice(pool) for _ in range(size)] for _ in days]


def _quantile(values, fraction):
    """Nearest-rank quantile of an already simulated list."""
    ordered = sorted(values)
    index = max(0, min(len(ordered) - 1, ceil(fraction * len(ordered)) - 1))
    return ordered[index]


def _values_for_day(days, batches, index):
    """Flatten one pending day's simulated workloads across every batch."""
    day = days[index]
    base = float(day["base_minutes"])
    count = int(day["job_count"])
    simulated = []
    for batch in batches:
        samples = batch["samples"]
        if not isinstance(samples, list) or len(samples) != len(days):
            raise ValueError("样本批次与待审日不一致")
        values = samples[index]
        if not isinstance(values, list) or len(values) != batch["size"]:
            raise ValueError("样本批次数量与记录不一致")
        simulated.extend(base - count * float(error) for error in values)
    return simulated


def probabilities(inputs, batches, reserved_minutes, iterations):
    """Recompute per-day overtime probabilities from already drawn samples."""
    if reserved_minutes < 1:
        raise ValueError("每日预留分钟必须为正")
    days = inputs["pending_days"]
    if not days:
        raise ValueError("没有待审工单，无法计算超时概率")
    ordered = sorted(batches, key=lambda item: item["batch_index"])
    total = sum(batch["size"] for batch in ordered)
    if [batch["batch_index"] for batch in ordered] != list(range(len(ordered))):
        raise ValueError("样本批次编号不连续")
    if total != iterations:
        raise ValueError("样本批次尚未算齐，不能发布概率")

    rows = []
    for index, day in enumerate(days):
        simulated = _values_for_day(days, ordered, index)
        if len(simulated) != total:
            raise ValueError("样本批次尚未算齐，不能发布概率")
        exceed = sum(value > reserved_minutes for value in simulated)
        rows.append({
            "job_date": day["job_date"],
            "job_count": int(day["job_count"]),
            "base_minutes": round(float(day["base_minutes"]), 2),
            "mean_minutes": round(fmean(simulated), 2),
            "p90_minutes": round(_quantile(simulated, 0.9), 2),
            "exceed_count": exceed,
            "probability_pct": round(exceed / total * 100, 2),
        })

    highest = max(rows, key=lambda row: row["probability_pct"])
    average = fmean(row["probability_pct"] for row in rows)
    pool = [
        {"job_date": item["job_date"], "job_count": item["job_count"],
         "mean_error": round(item["mean_error"], 6)}
        for item in inputs["day_errors"]
    ]
    headline = (f"每日预留 {reserved_minutes:g} 分钟，模拟 {total} 次："
                + "；".join(f"{row['job_date']} 超时概率 {row['probability_pct']:.1f}%" for row in rows)
                + "。")
    return {
        "state": "warning" if highest["probability_pct"] >= 50 else "ok",
        "headline": headline,
        "note": (f"抽样池为 {len(pool)} 个留出日的平均估算误差（估算减实耗），"
                 f"共 {len(ordered)} 个批次、{total} 次样本。"
                 "同一天工单在每次抽样中共用一个误差；改预留分钟沿用同一批样本重算。"),
        "reserved_minutes": reserved_minutes,
        "iterations": total,
        "batch_count": len(ordered),
        "metrics": [
            {"label": "每日预留（分钟）", "value": reserved_minutes,
             "detail": "模拟实耗合计严格超过该值计为超时"},
            {"label": "模拟次数", "value": total, "detail": f"{len(ordered)} 个已算批次"},
            {"label": "最高超时概率（%）", "value": highest["probability_pct"],
             "detail": f"{highest['job_date']} · 待审 {highest['job_count']} 条"},
            {"label": "平均超时概率（%）", "value": round(average, 2),
             "detail": f"{len(rows)} 个待审日的算术平均"},
        ],
        "tables": [
            {"title": "各待审日超时概率", "columns": [
                {"key": "job_date", "label": "待审日"},
                {"key": "job_count", "label": "待审工单"},
                {"key": "base_minutes", "label": "估算合计（分钟）"},
                {"key": "mean_minutes", "label": "模拟实耗均值（分钟）"},
                {"key": "p90_minutes", "label": "P90（分钟）"},
                {"key": "exceed_count", "label": "超时样本"},
                {"key": "probability_pct", "label": "超时概率（%）"},
            ], "rows": rows},
            {"title": "抽样池：留出日均估算误差", "columns": [
                {"key": "job_date", "label": "留出日"},
                {"key": "job_count", "label": "已完工工单"},
                {"key": "mean_error", "label": "平均估算误差（分钟）"},
            ], "rows": pool},
            {"title": "已算样本批次", "columns": [
                {"key": "batch_index", "label": "批次"},
                {"key": "size", "label": "样本数"},
            ], "rows": [{"batch_index": batch["batch_index"], "size": batch["size"]} for batch in ordered]},
        ],
        "charts": [{"title": "各待审日超时概率（%）", "kind": "bar", "series": [
            {"label": row["job_date"], "value": row["probability_pct"]} for row in rows
        ]}],
        "sections": [{
            "title": "模拟口径",
            "text": ("模拟实耗合计 = 估算合计 − 工单数 × 抽到的留出日均误差（估算减实耗）。"
                     "同一天的所有工单共用一次抽样误差，因此日合计会随工单数放大该误差。"
                     "超时概率是严格超过每日预留分钟数的样本占比，P90 为最近秩。"
                     "改预留分钟只换阈值，不重新抽样。"),
        }],
    }


def compose(estimation, simulation, meta):
    """Assemble the stored result so the page and the export share one revision."""
    state = "warning" if "warning" in {estimation["state"], simulation["state"]} else "ok"
    return {
        "state": state,
        "headline": f"{estimation['headline']} {simulation['headline']}",
        "note": f"{estimation['note']} {simulation['note']}",
        "metrics": estimation["metrics"] + simulation["metrics"],
        "tables": estimation["tables"] + simulation["tables"],
        "charts": estimation["charts"] + simulation["charts"],
        "sections": estimation["sections"] + simulation["sections"],
        "simulation_meta": meta,
    }
