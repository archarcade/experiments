#!/usr/bin/env python3
"""
Statistical analysis module for USENIX-grade experiment evaluation.

Implements:
- Bootstrap confidence intervals (percentile and BCa)
- Wilcoxon signed-rank test for paired comparisons
- Friedman test for repeated measures
- Holm-Bonferroni correction for multiple comparisons
- Effect size measures (Cliff's delta, Vargha-Delaney A12)

Reference:
- Efron & Tibshirani, "An Introduction to the Bootstrap"
- Cliff, N. (1993). Dominance statistics
- Vargha & Delaney (2000). A Critique and Improvement of A12
"""

from __future__ import annotations

import math
import random
import statistics
from collections.abc import Callable
from typing import Any

from pydantic import BaseModel
from tqdm import tqdm


class ConfidenceInterval(BaseModel):
    """Represents a confidence interval with point estimate."""

    point_estimate: float
    lower: float
    upper: float
    confidence_level: float = 0.95
    method: str = "bootstrap_percentile"
    n_bootstrap: int = 10000

    def __str__(self) -> str:
        return f"{self.point_estimate:.4f} [{self.lower:.4f}, {self.upper:.4f}] ({int(self.confidence_level * 100)}% CI)"


class StatisticalTestResult(BaseModel):
    """Result of a statistical significance test."""

    test_name: str
    statistic: float
    p_value: float
    significant: bool
    alpha: float = 0.05
    effect_size: float | None = None
    effect_size_name: str | None = None
    effect_size_interpretation: str | None = None

    def __str__(self) -> str:
        sig = "significant" if self.significant else "not significant"
        es = (
            f", effect size ({self.effect_size_name})={self.effect_size:.4f}"
            if self.effect_size is not None
            else ""
        )
        return f"{self.test_name}: statistic={self.statistic:.4f}, p={self.p_value:.4f} ({sig} at α={self.alpha}){es}"


class MultipleComparisonResult(BaseModel):
    """Result of multiple comparison correction."""

    original_p_values: list[float]
    adjusted_p_values: list[float]
    significant: list[bool]
    method: str
    alpha: float


class EffectSizeResult(BaseModel):
    """Result of effect size calculation."""

    effect_size: float
    name: str
    interpretation: str

    def __str__(self) -> str:
        return f"{self.name}={self.effect_size:.4f} ({self.interpretation})"


class RunLevelMetrics(BaseModel):
    """Aggregated metrics for a single experiment run."""

    run_id: str
    system: str  # "baseline" or "cedar"
    median_latency: float
    mean_latency: float
    p95_latency: float
    p99_latency: float
    qps: float
    error_rate: float = 0.0
    total_queries: int = 0
    duration_seconds: float = 0.0


# =============================================================================
# Bootstrap Confidence Intervals
# =============================================================================


def bootstrap_ci(
    data: list[float],
    statistic_func: Callable[[list[float]], float] = statistics.median,
    n_bootstrap: int = 10000,
    confidence_level: float = 0.95,
    method: str = "percentile",
    seed: int | None = None,
) -> ConfidenceInterval:
    """
    Compute bootstrap confidence interval for a statistic.

    Args:
        data: Sample data (list of floats)
        statistic_func: Function to compute statistic (e.g., statistics.median)
        n_bootstrap: Number of bootstrap resamples
        confidence_level: Confidence level (default 0.95 for 95% CI)
        method: "percentile" or "bca" (bias-corrected and accelerated)
        seed: Random seed for reproducibility

    Returns:
        ConfidenceInterval with point estimate, lower, and upper bounds
    """
    if not data:
        return ConfidenceInterval(
            point_estimate=0.0,
            lower=0.0,
            upper=0.0,
            confidence_level=confidence_level,
            method=method,
            n_bootstrap=n_bootstrap,
        )

    if len(data) == 1:
        val = data[0]
        return ConfidenceInterval(
            point_estimate=val,
            lower=val,
            upper=val,
            confidence_level=confidence_level,
            method=method,
            n_bootstrap=n_bootstrap,
        )

    if seed is not None:
        random.seed(seed)

    n = len(data)
    point_estimate = statistic_func(data)

    # Generate bootstrap samples
    bootstrap_stats = []

    iterator = range(n_bootstrap)
    if n_bootstrap >= 2000:
        iterator = tqdm(
            iterator,
            desc="Bootstrapping CI",
            leave=False,
            unit="samples",
            mininterval=1.0,
        )

    for _ in iterator:
        resample = random.choices(data, k=n)
        bootstrap_stats.append(statistic_func(resample))

    bootstrap_stats.sort()

    if method == "percentile":
        alpha = 1 - confidence_level
        lower_idx = int((alpha / 2) * n_bootstrap)
        upper_idx = int((1 - alpha / 2) * n_bootstrap) - 1
        lower = bootstrap_stats[max(0, lower_idx)]
        upper = bootstrap_stats[min(n_bootstrap - 1, upper_idx)]
    elif method == "bca":
        # Bias-corrected and accelerated (BCa) bootstrap
        lower, upper = _bca_ci(
            data, bootstrap_stats, statistic_func, point_estimate, confidence_level
        )
    else:
        raise ValueError(f"Unknown method: {method}")

    return ConfidenceInterval(
        point_estimate=point_estimate,
        lower=lower,
        upper=upper,
        confidence_level=confidence_level,
        method=f"bootstrap_{method}",
        n_bootstrap=n_bootstrap,
    )


def _bca_ci(
    data: list[float],
    bootstrap_stats: list[float],
    statistic_func: Callable[[list[float]], float],
    point_estimate: float,
    confidence_level: float,
) -> tuple[float, float]:
    """Compute BCa confidence interval bounds."""
    from math import erf, sqrt

    def norm_cdf(x: float) -> float:
        return 0.5 * (1 + erf(x / sqrt(2)))

    def norm_ppf(p: float) -> float:
        # Approximate inverse normal CDF using Abramowitz and Stegun approximation
        if p <= 0:
            return -float("inf")
        if p >= 1:
            return float("inf")
        if p == 0.5:
            return 0.0

        sign = 1 if p > 0.5 else -1
        p = 1 - p if p > 0.5 else p

        t = sqrt(-2 * math.log(p))
        c0, c1, c2 = 2.515517, 0.802853, 0.010328
        d1, d2, d3 = 1.432788, 0.189269, 0.001308
        result = t - (c0 + c1 * t + c2 * t * t) / (
            1 + d1 * t + d2 * t * t + d3 * t * t * t
        )
        return sign * result

    n = len(data)
    n_bootstrap = len(bootstrap_stats)

    # Bias correction factor (z0)
    count_below = sum(1 for s in bootstrap_stats if s < point_estimate)
    z0 = norm_ppf(count_below / n_bootstrap) if count_below > 0 else 0.0

    # Acceleration factor (a) using jackknife
    jackknife_stats = []
    for i in range(n):
        jackknife_sample = data[:i] + data[i + 1 :]
        if jackknife_sample:
            jackknife_stats.append(statistic_func(jackknife_sample))

    if jackknife_stats:
        jack_mean = statistics.mean(jackknife_stats)
        diffs = [jack_mean - j for j in jackknife_stats]
        sum_cubed = sum(d**3 for d in diffs)
        sum_squared = sum(d**2 for d in diffs)
        a = sum_cubed / (6 * (sum_squared**1.5)) if sum_squared > 0 else 0.0
    else:
        a = 0.0

    # Adjusted percentiles
    alpha = 1 - confidence_level
    z_alpha_lower = norm_ppf(alpha / 2)
    z_alpha_upper = norm_ppf(1 - alpha / 2)

    # BCa adjusted percentiles
    def adjust_percentile(z_alpha: float) -> float:
        denom = 1 - a * (z0 + z_alpha)
        if abs(denom) < 1e-10:
            return 0.5
        adjusted = z0 + (z0 + z_alpha) / denom
        return norm_cdf(adjusted)

    p_lower = adjust_percentile(z_alpha_lower)
    p_upper = adjust_percentile(z_alpha_upper)

    # Get bounds from bootstrap distribution
    lower_idx = max(0, min(n_bootstrap - 1, int(p_lower * n_bootstrap)))
    upper_idx = max(0, min(n_bootstrap - 1, int(p_upper * n_bootstrap)))

    return bootstrap_stats[lower_idx], bootstrap_stats[upper_idx]


def bootstrap_ci_median(data: list[float], **kwargs) -> ConfidenceInterval:
    """Convenience function for bootstrap CI of median."""
    return bootstrap_ci(data, statistics.median, **kwargs)


def bootstrap_ci_mean(data: list[float], **kwargs) -> ConfidenceInterval:
    """Convenience function for bootstrap CI of mean."""
    return bootstrap_ci(data, statistics.mean, **kwargs)


def bootstrap_ci_percentile(
    data: list[float], percentile: float = 95, **kwargs
) -> ConfidenceInterval:
    """Convenience function for bootstrap CI of a percentile (e.g., p95, p99)."""

    def percentile_func(d: list[float]) -> float:
        if not d:
            return 0.0
        sorted_d = sorted(d)
        idx = (percentile / 100) * (len(sorted_d) - 1)
        lower_idx = int(idx)
        upper_idx = min(lower_idx + 1, len(sorted_d) - 1)
        fraction = idx - lower_idx
        return sorted_d[lower_idx] * (1 - fraction) + sorted_d[upper_idx] * fraction

    return bootstrap_ci(data, percentile_func, **kwargs)


# =============================================================================
# Wilcoxon Signed-Rank Test (Paired, Nonparametric)
# =============================================================================


def wilcoxon_signed_rank_test(
    sample1: list[float],
    sample2: list[float],
    alpha: float = 0.05,
    alternative: str = "two-sided",
) -> StatisticalTestResult:
    """
    Wilcoxon signed-rank test for paired samples.

    This is the recommended nonparametric test for comparing two related
    samples (e.g., baseline vs Cedar on the same runs).

    Args:
        sample1: First sample (e.g., baseline run metrics)
        sample2: Second sample (e.g., Cedar run metrics)
        alpha: Significance level
        alternative: "two-sided", "greater", or "less"

    Returns:
        StatisticalTestResult with test statistic and p-value
    """
    if len(sample1) != len(sample2):
        raise ValueError("Samples must have equal length for paired test")

    n = len(sample1)
    if n < 6:
        # Need at least 6 pairs for meaningful test
        return StatisticalTestResult(
            test_name="Wilcoxon signed-rank",
            statistic=0.0,
            p_value=1.0,
            significant=False,
            alpha=alpha,
        )

    # Compute differences
    differences = [b - a for a, b in zip(sample1, sample2)]

    # Remove zero differences
    nonzero_diffs = [(abs(d), 1 if d > 0 else -1, d) for d in differences if d != 0]
    if not nonzero_diffs:
        return StatisticalTestResult(
            test_name="Wilcoxon signed-rank",
            statistic=0.0,
            p_value=1.0,
            significant=False,
            alpha=alpha,
        )

    # Rank by absolute value
    nonzero_diffs.sort(key=lambda x: x[0])

    # Assign ranks (handle ties by averaging)
    n_nonzero = len(nonzero_diffs)
    ranks: list[float] = []
    i = 0
    while i < n_nonzero:
        j = i
        while j < n_nonzero and nonzero_diffs[j][0] == nonzero_diffs[i][0]:
            j += 1
        avg_rank = (i + 1 + j) / 2  # Average rank for ties
        for _ in range(i, j):
            ranks.append(avg_rank)
        i = j

    # Compute signed ranks
    w_plus = sum(r for r, (_, sign, _) in zip(ranks, nonzero_diffs) if sign > 0)
    w_minus = sum(r for r, (_, sign, _) in zip(ranks, nonzero_diffs) if sign < 0)

    # Test statistic
    if alternative == "two-sided":
        w = min(w_plus, w_minus)
    elif alternative == "greater":
        w = w_minus
    else:  # less
        w = w_plus

    # Normal approximation for p-value (valid for n >= 10)
    mean_w = n_nonzero * (n_nonzero + 1) / 4
    std_w = math.sqrt(n_nonzero * (n_nonzero + 1) * (2 * n_nonzero + 1) / 24)

    if std_w > 0:
        z = (w - mean_w) / std_w
        # Approximate p-value using standard normal
        from math import erf, sqrt

        p_value = 0.5 * (1 + erf(z / sqrt(2)))
        if alternative == "two-sided":
            p_value = 2 * min(p_value, 1 - p_value)
    else:
        p_value = 1.0

    # Also compute effect size (Cliff's delta)
    effect = cliffs_delta(sample1, sample2)

    return StatisticalTestResult(
        test_name="Wilcoxon signed-rank",
        statistic=w,
        p_value=p_value,
        significant=p_value < alpha,
        alpha=alpha,
        effect_size=effect.effect_size,
        effect_size_name="Cliff's delta",
        effect_size_interpretation=effect.interpretation,
    )


# =============================================================================
# Mann-Whitney U Test (Independent Samples, Nonparametric)
# =============================================================================


def mann_whitney_u_test(
    sample1: list[float],
    sample2: list[float],
    alpha: float = 0.05,
    alternative: str = "two-sided",
) -> StatisticalTestResult:
    """
    Mann-Whitney U test for independent samples.

    Use this when samples are not paired (e.g., different runs on different machines).

    Args:
        sample1: First sample
        sample2: Second sample
        alpha: Significance level
        alternative: "two-sided", "greater", or "less"

    Returns:
        StatisticalTestResult with test statistic and p-value
    """
    n1, n2 = len(sample1), len(sample2)
    if n1 < 3 or n2 < 3:
        return StatisticalTestResult(
            test_name="Mann-Whitney U",
            statistic=0.0,
            p_value=1.0,
            significant=False,
            alpha=alpha,
        )

    # Combine and rank
    combined = [(v, 0) for v in sample1] + [(v, 1) for v in sample2]
    combined.sort(key=lambda x: x[0])

    # Assign ranks with tie handling
    ranks: list[float] = []
    i = 0
    while i < len(combined):
        j = i
        while j < len(combined) and combined[j][0] == combined[i][0]:
            j += 1
        avg_rank = (i + 1 + j) / 2
        for _ in range(i, j):
            ranks.append(avg_rank)
        i = j

    # Sum of ranks for sample1
    r1 = sum(r for r, (_, group) in zip(ranks, combined) if group == 0)

    # U statistic
    u1 = r1 - n1 * (n1 + 1) / 2
    u2 = n1 * n2 - u1
    u = min(u1, u2)

    # Normal approximation
    mean_u = n1 * n2 / 2
    std_u = math.sqrt(n1 * n2 * (n1 + n2 + 1) / 12)

    if std_u > 0:
        z = (u - mean_u) / std_u
        from math import erf, sqrt

        p_value = 0.5 * (1 + erf(z / sqrt(2)))
        if alternative == "two-sided":
            p_value = 2 * min(p_value, 1 - p_value)
    else:
        p_value = 1.0

    effect = cliffs_delta(sample1, sample2)

    return StatisticalTestResult(
        test_name="Mann-Whitney U",
        statistic=u,
        p_value=p_value,
        significant=p_value < alpha,
        alpha=alpha,
        effect_size=effect.effect_size,
        effect_size_name="Cliff's delta",
        effect_size_interpretation=effect.interpretation,
    )


# =============================================================================
# Friedman Test (Repeated Measures, Nonparametric)
# =============================================================================


def friedman_test(
    *samples: list[float],
    alpha: float = 0.05,
) -> StatisticalTestResult:
    """
    Friedman test for repeated measures across multiple conditions.

    Use this for comparing more than two conditions (e.g., multiple policy counts).

    Args:
        *samples: Multiple samples (one per condition), must be same length
        alpha: Significance level

    Returns:
        StatisticalTestResult with test statistic and p-value
    """
    k = len(samples)
    if k < 3:
        raise ValueError("Friedman test requires at least 3 conditions")

    n = len(samples[0])
    if any(len(s) != n for s in samples):
        raise ValueError("All samples must have equal length")

    if n < 3:
        return StatisticalTestResult(
            test_name="Friedman",
            statistic=0.0,
            p_value=1.0,
            significant=False,
            alpha=alpha,
        )

    # Rank within each block (subject)
    rank_sums = [0.0] * k
    for i in range(n):
        block = [(samples[j][i], j) for j in range(k)]
        block.sort(key=lambda x: x[0])

        # Assign ranks with tie handling
        ranks: list[float] = [0.0] * k
        idx = 0
        while idx < k:
            j = idx
            while j < k and block[j][0] == block[idx][0]:
                j += 1
            avg_rank = (idx + 1 + j) / 2
            for m in range(idx, j):
                ranks[block[m][1]] = avg_rank
            idx = j

        for j in range(k):
            rank_sums[j] += ranks[j]

    # Friedman statistic
    sum_sq = sum(r**2 for r in rank_sums)
    friedman_stat = (12 / (n * k * (k + 1))) * sum_sq - 3 * n * (k + 1)

    # Chi-square approximation for p-value
    df = k - 1
    p_value = _chi2_sf(friedman_stat, df)

    return StatisticalTestResult(
        test_name="Friedman",
        statistic=friedman_stat,
        p_value=p_value,
        significant=p_value < alpha,
        alpha=alpha,
    )


def _chi2_sf(x: float, df: int) -> float:
    """Survival function (1 - CDF) of chi-square distribution."""
    # Use incomplete gamma function approximation
    if x <= 0:
        return 1.0
    if df <= 0:
        return 0.0

    # Simple approximation for chi-square SF
    # Using Wilson-Hilferty transformation
    if df > 1:
        z = ((x / df) ** (1 / 3) - (1 - 2 / (9 * df))) / math.sqrt(2 / (9 * df))
        from math import erf, sqrt

        p = 0.5 * (1 - erf(z / sqrt(2)))
        return max(0.0, min(1.0, p))
    else:
        # For df=1, use direct calculation
        from math import erf, sqrt

        return 1 - erf(sqrt(x / 2))


# =============================================================================
# Effect Sizes
# =============================================================================


def cliffs_delta(sample1: list[float], sample2: list[float]) -> EffectSizeResult:
    """
    Cliff's delta effect size (nonparametric, robust).

    Measures the probability that a randomly selected value from sample2
    is greater than a randomly selected value from sample1, minus the
    reverse probability.

    Interpretation:
        |d| < 0.147: negligible
        0.147 <= |d| < 0.33: small
        0.33 <= |d| < 0.474: medium
        |d| >= 0.474: large

    Args:
        sample1: First sample (e.g., baseline)
        sample2: Second sample (e.g., treatment)

    Returns:
        EffectSizeResult with delta value and interpretation

    Note:
        Uses O(n log n) algorithm with binary search instead of O(n²) naive
        comparison to handle large sample sizes (e.g., 6000+ values).
    """
    import bisect

    if not sample1 or not sample2:
        return EffectSizeResult(
            effect_size=0.0, name="Cliff's delta", interpretation="undefined"
        )

    n1, n2 = len(sample1), len(sample2)

    # O(n log n) algorithm using binary search
    # Sort sample2 for efficient counting
    sorted_sample2 = sorted(sample2)

    greater = 0  # count of (a, b) pairs where b > a
    less = 0  # count of (a, b) pairs where b < a

    for a in sample1:
        # bisect_left: number of elements in sorted_sample2 < a
        count_less = bisect.bisect_left(sorted_sample2, a)
        # bisect_right: number of elements in sorted_sample2 <= a
        count_less_or_equal = bisect.bisect_right(sorted_sample2, a)
        count_less_or_equal - count_less
        count_greater = n2 - count_less_or_equal

        greater += count_greater  # b > a
        less += count_less  # b < a

    delta = (greater - less) / (n1 * n2)

    # Interpretation (Romano et al., 2006)
    abs_delta = abs(delta)
    if abs_delta < 0.147:
        interpretation = "negligible"
    elif abs_delta < 0.33:
        interpretation = "small"
    elif abs_delta < 0.474:
        interpretation = "medium"
    else:
        interpretation = "large"

    return EffectSizeResult(
        effect_size=delta, name="Cliff's delta", interpretation=interpretation
    )


def vargha_delaney_a12(sample1: list[float], sample2: list[float]) -> EffectSizeResult:
    """
    Vargha-Delaney A12 effect size measure.

    A12 represents the probability that a value from sample2 is greater
    than a value from sample1. A12 = 0.5 means no difference.

    Interpretation:
        A12 ≈ 0.5: no effect (equal distributions)
        0.56 <= A12 < 0.64 (or symmetric): small effect
        0.64 <= A12 < 0.71: medium effect
        A12 >= 0.71: large effect

    Args:
        sample1: First sample (e.g., baseline)
        sample2: Second sample (e.g., treatment)

    Returns:
        EffectSizeResult with A12 value and interpretation

    Note:
        Uses O(n log n) algorithm with binary search instead of O(n²) naive
        comparison to handle large sample sizes.
    """
    import bisect

    if not sample1 or not sample2:
        return EffectSizeResult(
            effect_size=0.5, name="Vargha-Delaney A12", interpretation="undefined"
        )

    n1, n2 = len(sample1), len(sample2)

    # O(n log n) algorithm using binary search
    # Sort sample2 for efficient counting
    sorted_sample2 = sorted(sample2)

    # Count times sample2 > sample1 (with 0.5 for ties)
    r = 0.0
    for a in sample1:
        # bisect_left: number of elements in sorted_sample2 < a
        count_less = bisect.bisect_left(sorted_sample2, a)
        # bisect_right: number of elements in sorted_sample2 <= a
        count_less_or_equal = bisect.bisect_right(sorted_sample2, a)
        count_equal = count_less_or_equal - count_less
        count_greater = n2 - count_less_or_equal

        r += count_greater + 0.5 * count_equal

    a12 = r / (n1 * n2)

    # Interpretation
    # Distance from 0.5 determines effect magnitude
    dist = abs(a12 - 0.5)
    if dist < 0.06:
        interpretation = "negligible"
    elif dist < 0.14:
        interpretation = "small"
    elif dist < 0.21:
        interpretation = "medium"
    else:
        interpretation = "large"

    return EffectSizeResult(
        effect_size=a12, name="Vargha-Delaney A12", interpretation=interpretation
    )


def cohens_d(sample1: list[float], sample2: list[float]) -> EffectSizeResult:
    """
    Cohen's d effect size (parametric, assumes normality).

    Interpretation:
        |d| < 0.2: negligible/small
        0.2 <= |d| < 0.5: small
        0.5 <= |d| < 0.8: medium
        |d| >= 0.8: large

    Args:
        sample1: First sample
        sample2: Second sample

    Returns:
        EffectSizeResult with d value and interpretation
    """
    if len(sample1) < 2 or len(sample2) < 2:
        return EffectSizeResult(
            effect_size=0.0, name="Cohen's d", interpretation="undefined"
        )

    mean1 = statistics.mean(sample1)
    mean2 = statistics.mean(sample2)
    var1 = statistics.variance(sample1)
    var2 = statistics.variance(sample2)
    n1, n2 = len(sample1), len(sample2)

    # Pooled standard deviation
    pooled_var = ((n1 - 1) * var1 + (n2 - 1) * var2) / (n1 + n2 - 2)
    pooled_std = math.sqrt(pooled_var) if pooled_var > 0 else 1.0

    d = (mean2 - mean1) / pooled_std

    abs_d = abs(d)
    if abs_d < 0.2:
        interpretation = "negligible"
    elif abs_d < 0.5:
        interpretation = "small"
    elif abs_d < 0.8:
        interpretation = "medium"
    else:
        interpretation = "large"

    return EffectSizeResult(
        effect_size=d, name="Cohen's d", interpretation=interpretation
    )


# =============================================================================
# Multiple Comparisons Correction
# =============================================================================


def holm_bonferroni_correction(
    p_values: list[float],
    alpha: float = 0.05,
) -> MultipleComparisonResult:
    """
    Holm-Bonferroni step-down method for multiple comparison correction.

    More powerful than standard Bonferroni while controlling family-wise error rate.

    Args:
        p_values: List of p-values from multiple tests
        alpha: Overall significance level

    Returns:
        MultipleComparisonResult with adjusted p-values and significance
    """
    m = len(p_values)
    if m == 0:
        return MultipleComparisonResult(
            original_p_values=[],
            adjusted_p_values=[],
            significant=[],
            method="Holm-Bonferroni",
            alpha=alpha,
        )

    # Sort p-values with original indices
    indexed = [(p, i) for i, p in enumerate(p_values)]
    indexed.sort(key=lambda x: x[0])

    adjusted = [0.0] * m
    significant = [False] * m

    # Holm-Bonferroni procedure
    prev_adjusted = 0.0
    for rank, (p, original_idx) in enumerate(indexed):
        # Adjusted p-value
        adj_p = p * (m - rank)
        # Enforce monotonicity: adjusted p-values must be non-decreasing
        adj_p = max(adj_p, prev_adjusted)
        adj_p = min(adj_p, 1.0)
        adjusted[original_idx] = adj_p
        significant[original_idx] = adj_p < alpha
        prev_adjusted = adj_p

    return MultipleComparisonResult(
        original_p_values=p_values,
        adjusted_p_values=adjusted,
        significant=significant,
        method="Holm-Bonferroni",
        alpha=alpha,
    )


def bonferroni_correction(
    p_values: list[float],
    alpha: float = 0.05,
) -> MultipleComparisonResult:
    """
    Standard Bonferroni correction (more conservative than Holm).

    Args:
        p_values: List of p-values
        alpha: Overall significance level

    Returns:
        MultipleComparisonResult with adjusted p-values
    """
    m = len(p_values)
    if m == 0:
        return MultipleComparisonResult(
            original_p_values=[],
            adjusted_p_values=[],
            significant=[],
            method="Bonferroni",
            alpha=alpha,
        )

    adjusted = [min(p * m, 1.0) for p in p_values]
    significant = [adj_p < alpha for adj_p in adjusted]

    return MultipleComparisonResult(
        original_p_values=p_values,
        adjusted_p_values=adjusted,
        significant=significant,
        method="Bonferroni",
        alpha=alpha,
    )


# =============================================================================
# Run-Level Aggregation Helpers
# =============================================================================


def compute_run_level_metrics(
    latencies: list[float],
    run_id: str,
    system: str,
    duration_seconds: float | None = None,
    error_count: int = 0,
) -> RunLevelMetrics:
    """
    Compute aggregate metrics for a single run.

    Args:
        latencies: List of query latencies (ms)
        run_id: Identifier for this run
        system: "baseline" or "cedar"
        duration_seconds: Total run duration
        error_count: Number of failed queries

    Returns:
        RunLevelMetrics with median, mean, p95, p99, and QPS
    """
    if not latencies:
        return RunLevelMetrics(
            run_id=run_id,
            system=system,
            median_latency=0.0,
            mean_latency=0.0,
            p95_latency=0.0,
            p99_latency=0.0,
            qps=0.0,
            error_rate=1.0 if error_count > 0 else 0.0,
            total_queries=0,
            duration_seconds=duration_seconds or 0.0,
        )

    sorted_latencies = sorted(latencies)
    n = len(sorted_latencies)

    median = sorted_latencies[n // 2]
    mean = sum(sorted_latencies) / n
    p95_idx = int(0.95 * (n - 1))
    p99_idx = int(0.99 * (n - 1))
    p95 = sorted_latencies[p95_idx]
    p99 = sorted_latencies[p99_idx]

    total = n + error_count
    if duration_seconds and duration_seconds > 0:
        qps = total / duration_seconds
    else:
        # Estimate from latencies (rough)
        total_time_sec = sum(sorted_latencies) / 1000.0
        qps = n / total_time_sec if total_time_sec > 0 else 0.0

    error_rate = error_count / total if total > 0 else 0.0

    return RunLevelMetrics(
        run_id=run_id,
        system=system,
        median_latency=median,
        mean_latency=mean,
        p95_latency=p95,
        p99_latency=p99,
        qps=qps,
        error_rate=error_rate,
        total_queries=n,
        duration_seconds=duration_seconds or 0.0,
    )


def compare_systems(
    baseline_runs: list[RunLevelMetrics],
    cedar_runs: list[RunLevelMetrics],
    metric: str = "median_latency",
    alpha: float = 0.05,
) -> dict[str, Any]:
    """
    Comprehensive statistical comparison of two systems across runs.

    Args:
        baseline_runs: List of metrics from baseline runs
        cedar_runs: List of metrics from Cedar runs
        metric: Which metric to compare (default: median_latency)
        alpha: Significance level

    Returns:
        Dictionary with CIs, test results, and effect sizes
    """
    baseline_values = [getattr(r, metric) for r in baseline_runs]
    cedar_values = [getattr(r, metric) for r in cedar_runs]

    # CIs for each system
    baseline_ci = bootstrap_ci_median(baseline_values)
    cedar_ci = bootstrap_ci_median(cedar_values)

    # Paired test if same number of runs
    if len(baseline_runs) == len(cedar_runs):
        test_result = wilcoxon_signed_rank_test(
            baseline_values, cedar_values, alpha=alpha
        )
    else:
        test_result = mann_whitney_u_test(baseline_values, cedar_values, alpha=alpha)

    # Effect sizes
    cliff = cliffs_delta(baseline_values, cedar_values)
    a12 = vargha_delaney_a12(baseline_values, cedar_values)

    # Overhead calculation
    baseline_median = statistics.median(baseline_values) if baseline_values else 0
    cedar_median = statistics.median(cedar_values) if cedar_values else 0

    is_throughput = metric in ("qps", "tps", "tpm")
    oh = calculate_overhead_metrics(
        baseline_median, cedar_median, is_throughput=is_throughput
    )

    return {
        "metric": metric,
        "n_baseline_runs": len(baseline_runs),
        "n_cedar_runs": len(cedar_runs),
        "baseline_ci": baseline_ci.model_dump(),
        "cedar_ci": cedar_ci.model_dump(),
        "test_result": test_result.model_dump(),
        "cliffs_delta": cliff.model_dump(),
        "vargha_delaney_a12": a12.model_dump(),
        "overhead_ms": cedar_median - baseline_median if not is_throughput else 0,
        "overhead_pct": oh["overhead_pct"],
        "overhead_factor": oh["overhead_factor"],
    }


# =============================================================================
# Utility Functions
# =============================================================================


def calculate_overhead_metrics(
    baseline: float, cedar: float, is_throughput: bool = False
) -> dict[str, float]:
    """
    Calculate consistent overhead metrics.

    If is_throughput=True, it converts throughput values to cost (latency-equivalent)
    to calculate overhead.

    Returns:
        {
            "overhead_pct": percentage increase in cost (latency),
            "overhead_factor": multiplier of baseline cost (e.g. 1.25x)
        }
    """
    if is_throughput:
        # T_base = baseline, T_cedar = cedar
        # Cost_base = 1/T_base, Cost_cedar = 1/T_cedar
        # Overhead Factor = Cost_cedar / Cost_base = (1/T_cedar) / (1/T_base) = T_base / T_cedar
        if cedar <= 0:
            return {"overhead_pct": float("inf"), "overhead_factor": float("inf")}
        overhead_factor = baseline / cedar
    else:
        # L_base = baseline, L_cedar = cedar
        # Overhead Factor = L_cedar / L_base
        if baseline <= 0:
            return {"overhead_pct": 0.0, "overhead_factor": 1.0}
        overhead_factor = cedar / baseline

    overhead_pct = (overhead_factor - 1.0) * 100.0
    return {"overhead_pct": overhead_pct, "overhead_factor": overhead_factor}


def percentile(data: list[float], p: float) -> float:
    """Compute p-th percentile of data."""
    if not data:
        return 0.0
    sorted_data = sorted(data)
    idx = (p / 100) * (len(sorted_data) - 1)
    lower = int(idx)
    upper = min(lower + 1, len(sorted_data) - 1)
    fraction = idx - lower
    return sorted_data[lower] * (1 - fraction) + sorted_data[upper] * fraction


def summary_stats(data: list[float]) -> dict[str, float]:
    """Compute summary statistics for a sample."""
    if not data:
        return {
            "n": 0,
            "mean": 0.0,
            "median": 0.0,
            "std": 0.0,
            "min": 0.0,
            "max": 0.0,
            "p25": 0.0,
            "p75": 0.0,
            "p95": 0.0,
            "p99": 0.0,
        }

    return {
        "n": len(data),
        "mean": statistics.mean(data),
        "median": statistics.median(data),
        "std": statistics.stdev(data) if len(data) > 1 else 0.0,
        "min": min(data),
        "max": max(data),
        "p25": percentile(data, 25),
        "p75": percentile(data, 75),
        "p95": percentile(data, 95),
        "p99": percentile(data, 99),
    }


def compute_overhead_with_ci(
    baseline_values: list[float],
    cedar_values: list[float],
    is_throughput: bool = False,
    n_bootstrap: int = 10000,
    confidence_level: float = 0.95,
) -> dict[str, Any]:
    """
    Compute overhead with bootstrap confidence intervals.

    This is useful for summarizing benchmark results with proper statistical rigor.

    Args:
        baseline_values: List of baseline measurements (latencies or throughputs)
        cedar_values: List of Cedar measurements (latencies or throughputs)
        is_throughput: If True, values are throughputs (higher is better)
        n_bootstrap: Number of bootstrap samples
        confidence_level: Confidence level for intervals (default 0.95 = 95%)

    Returns:
        Dictionary with:
            - baseline_ci: ConfidenceInterval for baseline
            - cedar_ci: ConfidenceInterval for Cedar
            - overhead_pct: Point estimate of overhead percentage
            - overhead_ci: ConfidenceInterval for overhead percentage
            - n_baseline: Number of baseline samples
            - n_cedar: Number of Cedar samples
            - significant: Whether difference is statistically significant
    """
    if not baseline_values or not cedar_values:
        return {
            "baseline_ci": None,
            "cedar_ci": None,
            "overhead_pct": 0.0,
            "overhead_ci": None,
            "n_baseline": len(baseline_values),
            "n_cedar": len(cedar_values),
            "significant": False,
            "error": "Insufficient data",
        }

    # Compute CIs for each system
    baseline_ci = bootstrap_ci_median(
        baseline_values, n_bootstrap=n_bootstrap, confidence_level=confidence_level
    )
    cedar_ci = bootstrap_ci_median(
        cedar_values, n_bootstrap=n_bootstrap, confidence_level=confidence_level
    )

    # Compute overhead
    oh = calculate_overhead_metrics(
        baseline_ci.point_estimate, cedar_ci.point_estimate, is_throughput=is_throughput
    )

    # Bootstrap CI for overhead percentage
    rng = random.Random(42)  # Reproducible
    overhead_samples = []

    overhead_samples = []

    # Use tqdm for progress indication if n_bootstrap is large
    # leave=False to clear the progress bar after completion
    iterator = range(n_bootstrap)
    if n_bootstrap >= 1000:
        iterator = tqdm(
            iterator, desc="Bootstrapping stats", leave=False, unit="samples"
        )

    for _ in iterator:
        # Resample both
        b_sample = [
            baseline_values[rng.randint(0, len(baseline_values) - 1)]
            for _ in range(len(baseline_values))
        ]
        c_sample = [
            cedar_values[rng.randint(0, len(cedar_values) - 1)]
            for _ in range(len(cedar_values))
        ]

        b_median = statistics.median(b_sample)
        c_median = statistics.median(c_sample)

        sample_oh = calculate_overhead_metrics(
            b_median, c_median, is_throughput=is_throughput
        )
        overhead_samples.append(sample_oh["overhead_pct"])

    overhead_samples.sort()
    alpha = 1 - confidence_level
    lower_idx = int(alpha / 2 * n_bootstrap)
    upper_idx = int((1 - alpha / 2) * n_bootstrap)

    overhead_ci = ConfidenceInterval(
        point_estimate=oh["overhead_pct"],
        lower=overhead_samples[lower_idx],
        upper=overhead_samples[upper_idx],
        confidence_level=confidence_level,
        method="bootstrap_percentile",
        n_bootstrap=n_bootstrap,
    )

    # Statistical test
    if len(baseline_values) == len(cedar_values) and len(baseline_values) >= 6:
        test = wilcoxon_signed_rank_test(baseline_values, cedar_values)
    else:
        test = mann_whitney_u_test(baseline_values, cedar_values)

    return {
        "baseline_ci": baseline_ci.model_dump(),
        "cedar_ci": cedar_ci.model_dump(),
        "overhead_pct": oh["overhead_pct"],
        "overhead_factor": oh["overhead_factor"],
        "overhead_ci": overhead_ci.model_dump(),
        "n_baseline": len(baseline_values),
        "n_cedar": len(cedar_values),
        "significant": test.significant,
        "p_value": test.p_value,
        "effect_size": test.effect_size,
        "effect_interpretation": test.effect_size_interpretation,
    }


def format_overhead_with_ci(result: dict[str, Any]) -> str:
    """Format overhead result for human-readable output."""
    if result.get("error"):
        return f"Error: {result['error']}"

    overhead_ci = result.get("overhead_ci", {})
    pct = result.get("overhead_pct", 0)
    lower = overhead_ci.get("lower", pct)
    upper = overhead_ci.get("upper", pct)

    sig = "***" if result.get("significant") else ""

    return f"{pct:+.2f}% [{lower:+.2f}%, {upper:+.2f}%] (95% CI){sig}"
