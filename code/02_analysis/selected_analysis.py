from config import ALL_STATE_DATA, TABLES
import pandas as pd
from linearmodels.panel import PanelOLS


def star_format(p):
    if p < 0.01:
        return "***"
    elif p < 0.05:
        return "**"
    elif p < 0.1:
        return "*"
    else:
        return ""


def run_models(df):
    df = df.copy()

    # ------------------------------------------------------------
    # Construct variables
    # ------------------------------------------------------------
    # Leap-year adjustment for share of year
    df["total_days"] = 365
    df.loc[df["year"] % 4 == 0, "total_days"] = 366

    # Outcomes
    df["inflow_rate"] = df["inflow"] / df["POP"] * 100000
    df["exit_rate"] = df["exits"] / df["POP"] * 100000

    # Policy variables
    df["share_year_active"] = df["overall_days"] / df["total_days"]

    df["scorecard_active"] = df["SCORECARD"]
    df.loc[df["overall_days"] == 0, "scorecard_active"] = 0

    df["moratorium_intensity"] = df["share_year_active"] * df["SCORECARD"]

    # Set panel index
    df = df.set_index(["state_code", "year"]).sort_index()

    controls = ["U3", "COVID_cases"]

    # Tables to make: filename -> (outcome variable, display title)
    table_specs = {
        "inflow_table.tex": ("inflow_rate", "Inflow to Homelessness"),
        "exits_table.tex": ("exit_rate", "Exits from Homelessness"),
        "median_days_table.tex": ("median_days_homeless", "Duration of Homelessness"),
    }

    # Column specs: variable name -> display label
    policy_specs = {
        "share_year_active": r"\shortstack{\\[0.1ex] Share of Year \\ with Active Moratorium}",
        "scorecard_active": r"\shortstack{\\[0.1ex] Housing Policy \\ Scorecard}",
        "moratorium_intensity": "Moratorium Intensity",
    }

    for filename, (outcome, outcome_label) in table_specs.items():
        results = {}

        for policy_var, policy_label in policy_specs.items():
            vars_needed = [outcome, policy_var] + controls
            df_clean = df[vars_needed].dropna()

            Y = df_clean[outcome]
            X = df_clean[[policy_var] + controls]

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

            beta = res.params.get(policy_var, float("nan"))
            pval = res.pvalues.get(policy_var, float("nan"))
            beta_str = f"{beta:.3f}{star_format(pval)}" if pd.notna(beta) else "omitted"

            results[policy_label] = [
                beta_str,
                f"{int(res.nobs)}",
                "Yes",
                "Yes",
                f"{res.rsquared_within:.3f}"
            ]

        table = pd.DataFrame(
            results,
            index=["Beta", "Observations", "State FE", "Year FE", "Within $R^2$"]
        )

        outpath = TABLES / filename

        table.to_latex(
            outpath,
            escape=False,
            index=True,
            column_format="lccc"
        )

        print(f"✓ Wrote {outpath}")


if __name__ == "__main__":
    df = pd.read_stata(ALL_STATE_DATA)
    run_models(df)