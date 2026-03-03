from .analysis_analytic import (
    categorize_query_complexity,
    collect_analytic_results,
    extract_analytic_results,
    generate_analytic_visualizations,
    generate_overhead_ratio_plot,
    write_analytic_summary_csv,
    write_analytic_summary_table_tex,
)
from .analysis_contention import (
    analyze_role_diversity_impact,
    collect_contention_results,
    extract_contention_results,
    generate_contention_visualizations,
    write_contention_summary_csv,
    write_contention_summary_table_tex,
)

# USENIX Paper Analysis Modules
from .analysis_ddl import (
    collect_ddl_results,
    extract_ddl_results,
    generate_ddl_visualizations,
    write_ddl_summary_csv,
    write_ddl_summary_table_tex,
)
from .analyzer import ResultsAnalyzer
from .benchmark_runner import BenchmarkRunner

# Benchmark user and authorization verification
from .benchmark_user_setup import (
    BENCHMARK_PASSWORD,
    BENCHMARK_USER,
    create_mysql_benchmark_user,
    create_postgres_benchmark_user,
    get_benchmark_credentials,
    verify_benchmark_user_access,
)
from .cedar_stats import (
    AuthorizationVerifier,
    check_cedar_agent_health,
    get_authorization_decision_breakdown,
    get_cedar_agent_stats,
    reset_cedar_agent_stats,
    verify_auth_invocations,
)
from .config import Config, load_config_file
from .connection_pool import ConnectionPool
from .data_generator import DataGenerator, get_generator
from .differential_profiling import (
    diff_profiles_to_csv,
    mysql_collect_stage_wait_profile,
    postgres_collect_explain_profile,
)
from .metadata import (
    ContainerInfo,
    ExperimentMetadata,
    GitInfo,
    HardwareInfo,
    MetadataCollector,
    SoftwareInfo,
    load_metadata,
)
from .multi_run import (
    MultiRunOrchestrator,
    MultiRunResult,
    OrderingStrategy,
    PairedRunResult,
    RunResult,
    run_experiment_batch,
)
from .overhead_breakdown import (
    OverheadBreakdownAnalyzer,
    OverheadBreakdownResult,
    PhaseBreakdown,
    RequestTimingRecord,
    generate_request_id,
)
from .query_generator import QueryGenerator, get_query_generator
from .schema_introspector import SchemaIntrospector

# New USENIX-grade evaluation modules
from .stats import (
    ConfidenceInterval,
    EffectSizeResult,
    RunLevelMetrics,
    StatisticalTestResult,
    bonferroni_correction,
    bootstrap_ci,
    bootstrap_ci_median,
    bootstrap_ci_percentile,
    cliffs_delta,
    cohens_d,
    compare_systems,
    compute_run_level_metrics,
    friedman_test,
    holm_bonferroni_correction,
    mann_whitney_u_test,
    vargha_delaney_a12,
    wilcoxon_signed_rank_test,
)
from .test_data_manager import TestDataManager
from .workload_cache import WorkloadCache
from .workload_generator import Query, Workload, WorkloadGenerator

__all__ = [
    # Original exports
    "DataGenerator",
    "get_generator",
    "QueryGenerator",
    "get_query_generator",
    "TestDataManager",
    "Config",
    "load_config_file",
    "WorkloadGenerator",
    "Workload",
    "Query",
    "BenchmarkRunner",
    "ResultsAnalyzer",
    "ConnectionPool",
    "SchemaIntrospector",
    "WorkloadCache",
    # Statistical analysis
    "bootstrap_ci",
    "bootstrap_ci_median",
    "bootstrap_ci_percentile",
    "wilcoxon_signed_rank_test",
    "mann_whitney_u_test",
    "friedman_test",
    "cliffs_delta",
    "vargha_delaney_a12",
    "cohens_d",
    "holm_bonferroni_correction",
    "bonferroni_correction",
    "compare_systems",
    "compute_run_level_metrics",
    "ConfidenceInterval",
    "StatisticalTestResult",
    "EffectSizeResult",
    "RunLevelMetrics",
    # Metadata
    "MetadataCollector",
    "ExperimentMetadata",
    "GitInfo",
    "ContainerInfo",
    "HardwareInfo",
    "SoftwareInfo",
    "load_metadata",
    # Multi-run orchestration
    "MultiRunOrchestrator",
    "MultiRunResult",
    "PairedRunResult",
    "RunResult",
    "OrderingStrategy",
    "run_experiment_batch",
    # Overhead breakdown
    "OverheadBreakdownAnalyzer",
    "OverheadBreakdownResult",
    "RequestTimingRecord",
    "PhaseBreakdown",
    "generate_request_id",
    # Differential profiling
    "mysql_collect_stage_wait_profile",
    "postgres_collect_explain_profile",
    "diff_profiles_to_csv",
    # DDL Analysis (E10)
    "extract_ddl_results",
    "collect_ddl_results",
    "write_ddl_summary_csv",
    "write_ddl_summary_table_tex",
    "generate_ddl_visualizations",
    # Analytic Query Analysis (E5)
    "categorize_query_complexity",
    "extract_analytic_results",
    "collect_analytic_results",
    "write_analytic_summary_csv",
    "write_analytic_summary_table_tex",
    "generate_analytic_visualizations",
    "generate_overhead_ratio_plot",
    # Contention Analysis (E6)
    "extract_contention_results",
    "collect_contention_results",
    "write_contention_summary_csv",
    "write_contention_summary_table_tex",
    "generate_contention_visualizations",
    "analyze_role_diversity_impact",
    # Benchmark User Setup
    "BENCHMARK_USER",
    "BENCHMARK_PASSWORD",
    "create_mysql_benchmark_user",
    "create_postgres_benchmark_user",
    "verify_benchmark_user_access",
    "get_benchmark_credentials",
    # Cedar Stats & Authorization Verification
    "get_cedar_agent_stats",
    "reset_cedar_agent_stats",
    "verify_auth_invocations",
    "check_cedar_agent_health",
    "get_authorization_decision_breakdown",
    "AuthorizationVerifier",
]
