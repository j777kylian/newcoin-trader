"""Bounded human-readable 1/2-condition candidate rules (train→val→test once)."""

from __future__ import annotations

import json
from collections.abc import Sequence
from decimal import Decimal
from hashlib import sha256

from newcoin_trader.domain.event_study import CellOutcomeStatus
from newcoin_trader.domain.feature_research import (
    CandidateRule,
    DecisionFeatureRecord,
    FeatureValueState,
    RuleCondition,
    RuleSelectionResult,
)
from newcoin_trader.research.event_study_aggregate import deterministic_quantile


def _numeric_feature_map(record: DecisionFeatureRecord) -> dict[str, Decimal]:
    out: dict[str, Decimal] = {}
    for feat in record.features:
        if feat.state is not FeatureValueState.AVAILABLE:
            continue
        if isinstance(feat.value, Decimal):
            out[feat.name] = feat.value
        elif isinstance(feat.value, str):
            try:
                out[feat.name] = Decimal(feat.value)
            except Exception:
                continue
    return out


def _primary_return(record: DecisionFeatureRecord) -> Decimal | None:
    for label in record.labels:
        if label.status is CellOutcomeStatus.COMPLETE and label.simple_return is not None:
            return label.simple_return
    return None


def _condition_holds(condition: RuleCondition, features: dict[str, Decimal]) -> bool:
    if condition.feature_name not in features:
        return False
    value = features[condition.feature_name]
    threshold = condition.threshold if isinstance(condition.threshold, Decimal) else Decimal(str(condition.threshold))
    if condition.op == "gt":
        return value > threshold
    if condition.op == "gte":
        return value >= threshold
    if condition.op == "lt":
        return value < threshold
    if condition.op == "lte":
        return value <= threshold
    if condition.op == "eq":
        return value == threshold
    return False


def evaluate_rule(
    rule: CandidateRule,
    records: Sequence[DecisionFeatureRecord],
) -> tuple[DecisionFeatureRecord, ...]:
    matched: list[DecisionFeatureRecord] = []
    for record in records:
        feats = _numeric_feature_map(record)
        if all(_condition_holds(cond, feats) for cond in rule.conditions):
            matched.append(record)
    return tuple(matched)


def _mean_return(records: Sequence[DecisionFeatureRecord]) -> Decimal | None:
    values = [r for r in (_primary_return(rec) for rec in records) if r is not None]
    if not values:
        return None
    return sum(values, start=Decimal("0")) / Decimal(len(values))


def _human_readable(conditions: Sequence[RuleCondition]) -> str:
    parts: list[str] = []
    for cond in conditions:
        thr = str(cond.threshold)
        parts.append(f"{cond.feature_name} {cond.op} {thr}")
    return " AND ".join(parts)


def _rule_id(conditions: Sequence[RuleCondition]) -> str:
    payload = [{"feature_name": c.feature_name, "op": c.op, "threshold": str(c.threshold)} for c in conditions]
    digest = sha256(json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    return digest[:16]


def discover_candidate_rules(
    train: Sequence[DecisionFeatureRecord],
    *,
    max_rules: int,
    max_conditions: int,
    min_sample: int,
) -> tuple[CandidateRule, ...]:
    """Generate capped 1/2-condition rules from train medians/quantiles only."""
    if max_conditions < 1 or max_rules < 1:
        return ()
    # Collect feature universes from train.
    feature_values: dict[str, list[Decimal]] = {}
    for record in train:
        for name, value in _numeric_feature_map(record).items():
            feature_values.setdefault(name, []).append(value)
    feature_names = sorted(feature_values.keys())
    # Deterministic candidate thresholds per feature.
    thresholds: dict[str, list[Decimal]] = {}
    for name in feature_names:
        ordered = sorted(feature_values[name])
        edges: list[Decimal] = []
        for q in (Decimal("0.25"), Decimal("0.5"), Decimal("0.75")):
            edge = deterministic_quantile(ordered, q)
            if edge is not None and edge not in edges:
                edges.append(edge)
        thresholds[name] = edges

    single_conditions: list[RuleCondition] = []
    for name in feature_names:
        for thr in thresholds.get(name, []):
            for op in ("gt", "gte", "lt", "lte"):
                single_conditions.append(RuleCondition(feature_name=name, op=op, threshold=thr))

    candidates: list[CandidateRule] = []
    seen_ids: set[str] = set()

    def _try_add(conds: tuple[RuleCondition, ...]) -> None:
        if len(candidates) >= max_rules:
            return
        rid = _rule_id(conds)
        if rid in seen_ids:
            return
        rule = CandidateRule(
            rule_id=rid,
            conditions=conds,
            human_readable=_human_readable(conds),
        )
        matched = evaluate_rule(rule, train)
        if len(matched) < min_sample:
            return
        mean_ret = _mean_return(matched)
        seen_ids.add(rid)
        candidates.append(
            CandidateRule(
                rule_id=rid,
                conditions=conds,
                human_readable=rule.human_readable,
                train_event_ids=tuple(r.event_id for r in matched),
                train_sample_count=len(matched),
                train_mean_return=mean_ret,
            )
        )

    # Prefer one-condition rules first (deterministic order).
    for cond in single_conditions:
        if len(candidates) >= max_rules:
            break
        _try_add((cond,))

    if max_conditions >= 2 and len(candidates) < max_rules:
        # Bounded pairwise: first N singles × later singles (feature name order).
        singles = single_conditions[: min(32, len(single_conditions))]
        for i, left in enumerate(singles):
            for right in singles[i + 1 :]:
                if left.feature_name == right.feature_name:
                    continue
                if len(candidates) >= max_rules:
                    break
                # Deterministic condition order by feature name.
                pair = tuple(sorted((left, right), key=lambda c: (c.feature_name, c.op, str(c.threshold))))
                _try_add(pair)
            if len(candidates) >= max_rules:
                break

    # Rank by train mean return descending, then rule_id (deterministic).
    candidates.sort(
        key=lambda r: (
            -(r.train_mean_return if r.train_mean_return is not None else Decimal("-Infinity")),
            r.rule_id,
        )
    )
    return tuple(candidates[:max_rules])


def select_and_test_rules(
    *,
    train: Sequence[DecisionFeatureRecord],
    validation: Sequence[DecisionFeatureRecord],
    test: Sequence[DecisionFeatureRecord],
    max_rules: int,
    max_conditions: int,
    min_sample: int,
) -> RuleSelectionResult:
    candidates = discover_candidate_rules(
        train,
        max_rules=max_rules,
        max_conditions=max_conditions,
        min_sample=min_sample,
    )
    # Select on validation only.
    scored: list[CandidateRule] = []
    for rule in candidates:
        matched_val = evaluate_rule(rule, validation)
        val_mean = _mean_return(matched_val)
        scored.append(
            CandidateRule(
                rule_id=rule.rule_id,
                conditions=rule.conditions,
                human_readable=rule.human_readable,
                train_event_ids=rule.train_event_ids,
                train_sample_count=rule.train_sample_count,
                train_mean_return=rule.train_mean_return,
                validation_mean_return=val_mean,
            )
        )
    scored.sort(
        key=lambda r: (
            -(r.validation_mean_return if r.validation_mean_return is not None else Decimal("-Infinity")),
            r.rule_id,
        )
    )
    # Select top half (at least 1 if any) — still bounded by max_rules.
    select_n = max(1, len(scored) // 2) if scored else 0
    selected_base = scored[:select_n]
    # Evaluate test exactly once.
    selected: list[CandidateRule] = []
    for rule in selected_base:
        matched_test = evaluate_rule(rule, test)
        test_mean = _mean_return(matched_test)
        selected.append(
            CandidateRule(
                rule_id=rule.rule_id,
                conditions=rule.conditions,
                human_readable=rule.human_readable,
                train_event_ids=rule.train_event_ids,
                train_sample_count=rule.train_sample_count,
                train_mean_return=rule.train_mean_return,
                validation_mean_return=rule.validation_mean_return,
                test_mean_return=test_mean,
                selected=True,
            )
        )
    return RuleSelectionResult(
        candidates=tuple(scored),
        selected=tuple(selected),
        test_evaluated_once=True,
    )
