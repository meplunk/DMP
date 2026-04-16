from pathlib import Path

import pandas as pd
import matplotlib.pyplot as plt
from linearmodels.panel import PanelOLS
from typing import List, Optional

from config import ALL_STATE_DATA, TABLES, TO_SHARE, CONTROLS, VAR_LABELS


# =========================
# SETTINGS
# =========================

OUTCOMES = [
    "inflow_rate",
    "median_days_homeless",
    "exit_rate",
    "perm_exit_rate"
]

FOCUS_POLICIES = [
    "moratorium_intensity",
    "share_moratorium_intensity",
    "overall_days",
    "weighted_scorecard"
]

USE_STATE_TRENDS = False   # set True if you want leave-one-out with state-specific linear trends
SAVE_CSV = True
SAVE_PLOTS = True

LEAVE_ONE_OUT_DIR = TABLES / "leave_one_out"
LEAVE_ONE_OUT_DIR.mkdir(parents=True, exist_ok=True)


# =========================
# HELPERS
# =========================

def prepare_data(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    df["total_days"] = 365
    df.loc[df["year"] % 4 == 0, "total_days"] = 366

    # centered time variable for optional state-specific trends
    df["t"] = df["year"] - df["year"].min()

    # Create share variables
    for var in TO_SHARE:
        share_var = f"share_{var}"
        df[share_var] = df[var] / df["total_days"]

    # Outcome variables
    df["inflow_rate"] = df["inflow"] / df["POP"] * 100000
    df["exit_rate"] = df["exits"] / df["POP"] * 100000
    df["perm_exit_rate"] = df["exits_perm"] / df["POP"] * 100000

    # Policy intensity variables
    df["moratorium_intensity"] = df["overall_days"] * df["SCORECARD"]
    df["share_moratorium_intensity"] = df["share_overall_days"] * df["SCORECARD"]

    df["weighted_scorecard"] = df["SCORECARD"]
    df.loc[df["overall_days"] == 0, "weighted_scorecard"] = 0

    return df


def add_state_linear_trends(df: pd.DataFrame) -> tuple[pd.DataFrame, list[str]]:
    """
    Create manual state-specific linear trends:
    trend_STATE = 1[state == STATE] * t

    One state is omitted as the reference category.
    """
    df = df.copy()

    states = sorted(df["state_code"].dropna().unique())
    trend_states = states[1:]   # omit first state as reference

    trend_vars = []
    for state in trend_states:
        trend_name = f"trend_{state}"
        df[trend_name] = (df["state_code"] == state).astype(int) * df["t"]
        trend_vars.append(trend_name)

    return df, trend_vars


def run_single_regression(
    df_panel: pd.DataFrame,
    outcome: str,
    policy: str,
    controls: list[str],
    trend_vars: Optional[List[str]] = None
) -> dict:
    """
    Run a single PanelOLS regression and return coefficient + SE + CI + p-value.
    Assumes df_panel is already indexed by ['state_code', 'year'].
    """
    if trend_vars is None:
        trend_vars = []

    vars_needed = [outcome, policy] + controls + trend_vars
    df_clean = df_panel[vars_needed].dropna()

    Y = df_clean[outcome]
    X = df_clean[[policy] + controls + trend_vars]

    model = PanelOLS(
        Y,
        X,
        entity_effects=True,
        time_effects=True,
        drop_absorbed=True
    )

    res = model.fit(
        cov_type="clustered",
        cluster_entity=True
    )

    coef = res.params.get(policy, float("nan"))
    se = res.std_errors.get(policy, float("nan"))
    pval = res.pvalues.get(policy, float("nan"))

    return {
        "coef": coef,
        "se": se,
        "pval": pval,
        "ci_lower": coef - 1.96 * se if pd.notnull(coef) and pd.notnull(se) else float("nan"),
        "ci_upper": coef + 1.96 * se if pd.notnull(coef) and pd.notnull(se) else float("nan"),
        "nobs": int(res.nobs),
        "r2_within": res.rsquared_within
    }


def leave_one_out_analysis(
    df_panel: pd.DataFrame,
    outcome: str,
    policy: str,
    controls: list[str],
    trend_vars: Optional[List[str]] = None
) -> pd.DataFrame:
    """
    Drop one state at a time, re-estimate the model, and store the results.
    """
    if trend_vars is None:
        trend_vars = []

    states = sorted(df_panel.index.get_level_values("state_code").unique())

    # Full-sample estimate
    full = run_single_regression(
        df_panel=df_panel,
        outcome=outcome,
        policy=policy,
        controls=controls,
        trend_vars=trend_vars
    )

    rows = []
    for state in states:
        keep_mask = df_panel.index.get_level_values("state_code") != state
        df_subset = df_panel.loc[keep_mask].copy()

        try:
            est = run_single_regression(
                df_panel=df_subset,
                outcome=outcome,
                policy=policy,
                controls=controls,
                trend_vars=trend_vars
            )

            rows.append({
                "state_dropped": state,
                "coef": est["coef"],
                "se": est["se"],
                "pval": est["pval"],
                "ci_lower": est["ci_lower"],
                "ci_upper": est["ci_upper"],
                "nobs": est["nobs"],
                "r2_within": est["r2_within"],
                "full_sample_coef": full["coef"],
                "full_sample_se": full["se"],
                "full_sample_pval": full["pval"],
                "full_sample_ci_lower": full["ci_lower"],
                "full_sample_ci_upper": full["ci_upper"],
                "coef_change": est["coef"] - full["coef"],
                "abs_coef_change": abs(est["coef"] - full["coef"]),
                "sign_flip": (est["coef"] * full["coef"] < 0)
            })
        except Exception as e:
            rows.append({
                "state_dropped": state,
                "coef": float("nan"),
                "se": float("nan"),
                "pval": float("nan"),
                "ci_lower": float("nan"),
                "ci_upper": float("nan"),
                "nobs": float("nan"),
                "r2_within": float("nan"),
                "full_sample_coef": full["coef"],
                "full_sample_se": full["se"],
                "full_sample_pval": full["pval"],
                "full_sample_ci_lower": full["ci_lower"],
                "full_sample_ci_upper": full["ci_upper"],
                "coef_change": float("nan"),
                "abs_coef_change": float("nan"),
                "sign_flip": float("nan"),
                "error": str(e)
            })

    return pd.DataFrame(rows)


def make_leave_one_out_plot(
    loo_df: pd.DataFrame,
    outcome: str,
    policy: str,
    outdir: Path,
    use_state_trends: bool = False
) -> None:
    """
    Plot leave-one-out coefficient estimates with 95% CI and full-sample reference line.
    """
    plot_df = loo_df.dropna(subset=["coef"]).copy()
    plot_df = plot_df.sort_values("coef").reset_index(drop=True)

    fig_height = max(8, 0.25 * len(plot_df))
    fig, ax = plt.subplots(figsize=(10, fig_height))

    y_positions = range(len(plot_df))

    # Confidence intervals
    ax.hlines(
        y=y_positions,
        xmin=plot_df["ci_lower"],
        xmax=plot_df["ci_upper"],
        linewidth=1
    )

    # Point estimates
    ax.scatter(plot_df["coef"], y_positions, s=30)

    # Full-sample estimate
    full_coef = plot_df["full_sample_coef"].iloc[0]
    ax.axvline(full_coef, linestyle="--", linewidth=1.5)

    # Labels
    ax.set_yticks(list(y_positions))
    ax.set_yticklabels(plot_df["state_dropped"])

    outcome_label = VAR_LABELS.get(outcome, outcome)
    policy_label = VAR_LABELS.get(policy, policy)

    trend_text = " + State Trends" if use_state_trends else ""
    ax.set_title(f"Leave-One-Out Analysis{trend_text}\n{policy_label} → {outcome_label}")
    ax.set_xlabel("Coefficient estimate")
    ax.set_ylabel("State dropped")

    plt.tight_layout()

    filename = f"loo_{policy}_{outcome}"
    if use_state_trends:
        filename += "_state_trends"
    filename += ".png"

    plt.savefig(outdir / filename, dpi=300, bbox_inches="tight")
    plt.close()


def make_summary_table(all_results: list[pd.DataFrame], outdir: Path, use_state_trends: bool = False) -> pd.DataFrame:
    """
    Combine all leave-one-out results and create a compact summary table showing
    the biggest movers for each policy/outcome.
    """
    combined = pd.concat(all_results, ignore_index=True)

    summary = (
        combined
        .dropna(subset=["abs_coef_change"])
        .sort_values(["policy", "outcome", "abs_coef_change"], ascending=[True, True, False])
        .groupby(["policy", "outcome"], as_index=False)
        .head(5)
        .copy()
    )

    summary["policy_label"] = summary["policy"].map(lambda x: VAR_LABELS.get(x, x))
    summary["outcome_label"] = summary["outcome"].map(lambda x: VAR_LABELS.get(x, x))

    cols = [
        "policy", "policy_label",
        "outcome", "outcome_label",
        "state_dropped",
        "coef", "full_sample_coef",
        "coef_change", "abs_coef_change",
        "sign_flip"
    ]
    summary = summary[cols]

    suffix = "_state_trends" if use_state_trends else ""
    summary.to_csv(outdir / f"leave_one_out_summary_top5{suffix}.csv", index=False)

    return summary


# =========================
# MAIN
# =========================

def main():
    df = pd.read_stata(ALL_STATE_DATA)
    df = prepare_data(df)

    if USE_STATE_TRENDS:
        df, trend_vars = add_state_linear_trends(df)
    else:
        trend_vars = []

    # Set panel index
    df_panel = df.set_index(["state_code", "year"]).sort_index()

    all_results = []

    for policy in FOCUS_POLICIES:
        for outcome in OUTCOMES:
            print(f"Running leave-one-out for {policy} -> {outcome}")

            loo_df = leave_one_out_analysis(
                df_panel=df_panel,
                outcome=outcome,
                policy=policy,
                controls=CONTROLS,
                trend_vars=trend_vars
            )

            loo_df["policy"] = policy
            loo_df["outcome"] = outcome
            all_results.append(loo_df)

            if SAVE_CSV:
                suffix = "_state_trends" if USE_STATE_TRENDS else ""
                csv_path = LEAVE_ONE_OUT_DIR / f"loo_{policy}_{outcome}{suffix}.csv"
                loo_df.to_csv(csv_path, index=False)

            if SAVE_PLOTS:
                make_leave_one_out_plot(
                    loo_df=loo_df,
                    outcome=outcome,
                    policy=policy,
                    outdir=LEAVE_ONE_OUT_DIR,
                    use_state_trends=USE_STATE_TRENDS
                )

    summary = make_summary_table(
        all_results=all_results,
        outdir=LEAVE_ONE_OUT_DIR,
        use_state_trends=USE_STATE_TRENDS
    )

    print("\nDone.")
    print("Top movers summary:")
    print(summary.head(20))


if __name__ == "__main__":
    main()