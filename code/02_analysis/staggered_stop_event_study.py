from pathlib import Path
from typing import List, Dict

import pandas as pd
import matplotlib.pyplot as plt
from linearmodels.panel import PanelOLS

from config import ALL_STATE_DATA, TABLES, CONTROLS, TO_SHARE, VAR_LABELS, OUTCOMES


# ============================================================
# SETTINGS
# ============================================================

EVENT_STUDY_DIR = TABLES / "event_study"
EVENT_STUDY_DIR.mkdir(parents=True, exist_ok=True)


# ============================================================
# HELPERS
# ============================================================

def star_format(p):
    if p < 0.01:
        return "***"
    elif p < 0.05:
        return "**"
    elif p < 0.1:
        return "*"
    else:
        return ""


def prepare_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Minimal prep for event study.
    Assumes ALL_STATE_DATA already contains:
      - state_code
      - year
      - overall_end_year
      - ever_treated
      - controls + outcomes
    """
    df = df.copy()

    # Keep only ever-treated states for rollback timing analysis
    df = df[df["ever_treated"] == 1].copy()

    # Need nonmissing rollback year
    df = df[df["overall_end_year"].notna()].copy()

    # event time relative to final overall moratorium end year
    df["event_time"] = df["year"] - df["overall_end_year"]

    return df


def make_event_dummies(df: pd.DataFrame) -> pd.DataFrame:
    """
    Create event-time dummy variables with binned leads/lags.

    Baseline omitted category: event_time == -1
    Included regressors:
        event_m3plus : event_time <= -3
        event_m2     : event_time == -2
        event_0      : event_time == 0
        event_p1     : event_time == 1
        event_p2plus : event_time >= 2
    """
    df = df.copy()

    df["event_m3plus"] = (df["event_time"] <= -3).astype(int)
    df["event_m2"] = (df["event_time"] == -2).astype(int)
    # omit -1
    df["event_0"] = (df["event_time"] == 0).astype(int)
    df["event_p1"] = (df["event_time"] == 1).astype(int)
    df["event_p2plus"] = (df["event_time"] >= 2).astype(int)

    return df


def run_event_study_regression(
    df_panel: pd.DataFrame,
    outcome: str,
    controls: List[str]
) -> Dict:
    """
    Run PanelOLS event study:
        outcome ~ event dummies + controls + state FE + year FE
    clustered by state (entity)
    """
    event_vars = ["event_m3plus", "event_m2", "event_0", "event_p1", "event_p2plus"]

    vars_needed = [outcome] + event_vars + controls
    df_clean = df_panel[vars_needed].dropna()

    Y = df_clean[outcome]
    X = df_clean[event_vars + controls]

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

    rows = []
    mapping = {
        "event_m3plus": -3,
        "event_m2": -2,
        "event_0": 0,
        "event_p1": 1,
        "event_p2plus": 2
    }

    for var in event_vars:
        if var in res.params.index:
            coef = res.params[var]
            se = res.std_errors[var]
            pval = res.pvalues[var]
        else:
            coef = float("nan")
            se = float("nan")
            pval = float("nan")

        rows.append({
            "term": var,
            "k": mapping[var],
            "coef": coef,
            "se": se,
            "pval": pval,
            "ci_lower": coef - 1.96 * se if pd.notnull(coef) and pd.notnull(se) else float("nan"),
            "ci_upper": coef + 1.96 * se if pd.notnull(coef) and pd.notnull(se) else float("nan"),
            "label": {
                -3: r"$\leq -3$",
                -2: "-2",
                0: "0",
                1: "1",
                2: r"$\geq 2$"
            }[mapping[var]]
        })

    coef_df = pd.DataFrame(rows).sort_values("k")

    return {
        "res": res,
        "coef_df": coef_df,
        "nobs": int(res.nobs),
        "r2_within": res.rsquared_within
    }


def save_event_study_table(coef_df: pd.DataFrame, outcome: str, outdir: Path) -> None:
    """
    Save coefficient table as CSV.
    """
    out = coef_df.copy()
    out["stars"] = out["pval"].apply(star_format)
    out["coef_fmt"] = out.apply(
        lambda r: f"{r['coef']:.3f}{r['stars']}" if pd.notnull(r["coef"]) else "",
        axis=1
    )
    out["se_fmt"] = out["se"].apply(lambda x: f"({x:.3f})" if pd.notnull(x) else "")

    outcome_name = VAR_LABELS.get(outcome, outcome)
    outpath = outdir / f"event_study_{outcome}.csv"
    out.to_csv(outpath, index=False)

    print(f"Saved coefficients for {outcome_name} to {outpath}")


def plot_event_study(coef_df: pd.DataFrame, outcome: str, outdir: Path) -> None:
    """
    Make event study coefficient plot.
    """
    fig, ax = plt.subplots(figsize=(8, 5))

    # plot points and CI
    ax.errorbar(
        coef_df["k"],
        coef_df["coef"],
        yerr=1.96 * coef_df["se"],
        fmt="o",
        capsize=4
    )

    # zero lines
    ax.axhline(0, linestyle="--", linewidth=1)
    ax.axvline(-1, linestyle=":", linewidth=1)

    # x ticks
    ax.set_xticks(coef_df["k"])
    ax.set_xticklabels(coef_df["label"])

    outcome_name = VAR_LABELS.get(outcome, outcome)
    ax.set_title(f"Event Study: Rollback of Overall Moratorium\nOutcome: {outcome_name}")
    ax.set_xlabel("Event time relative to moratorium end year\n(omitted baseline = -1)")
    ax.set_ylabel("Coefficient")

    plt.tight_layout()

    outpath = outdir / f"event_study_{outcome}.png"
    plt.savefig(outpath, dpi=300, bbox_inches="tight")
    plt.close()

    print(f"Saved plot for {outcome_name} to {outpath}")


def make_summary_file(results: Dict[str, Dict], outdir: Path) -> None:
    """
    Save one stacked summary CSV across outcomes.
    """
    all_rows = []
    for outcome, obj in results.items():
        temp = obj["coef_df"].copy()
        temp["outcome"] = outcome
        temp["outcome_label"] = VAR_LABELS.get(outcome, outcome)
        temp["nobs"] = obj["nobs"]
        temp["r2_within"] = obj["r2_within"]
        all_rows.append(temp)

    summary = pd.concat(all_rows, ignore_index=True)
    summary.to_csv(outdir / "event_study_all_outcomes.csv", index=False)
    print(f"Saved combined summary to {outdir / 'event_study_all_outcomes.csv'}")


# ============================================================
# MAIN
# ============================================================

def main():
    print("Loading state panel...")
    df = pd.read_stata(ALL_STATE_DATA)

    print("Preparing event-study sample...")
    df = prepare_data(df)
    df = make_event_dummies(df)

    # quick sanity checks
    print("\nStates in event-study sample:", df["state_code"].nunique())
    print("Years in sample:", sorted(df["year"].unique()))
    print("\nEvent time distribution:")
    print(df["event_time"].value_counts().sort_index())

    # PanelOLS wants a multi-index
    df_panel = df.set_index(["state_code", "year"]).sort_index()

    results = {}

    for outcome in OUTCOMES:
        print(f"\nRunning event study for {outcome}...")

        out = run_event_study_regression(
            df_panel=df_panel,
            outcome=outcome,
            controls=CONTROLS
        )

        results[outcome] = out

        print(f"N = {out['nobs']}, within R^2 = {out['r2_within']:.3f}")
        print(out["coef_df"][["label", "coef", "se", "pval"]])

        save_event_study_table(out["coef_df"], outcome, EVENT_STUDY_DIR)
        plot_event_study(out["coef_df"], outcome, EVENT_STUDY_DIR)

    make_summary_file(results, EVENT_STUDY_DIR)

    print("\nDone.")


if __name__ == "__main__":
    main()