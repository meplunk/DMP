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

outcomes = [
    "inflow_rate",
    "avg_days_homeless",
    "median_days_homeless",
    "exit_rate",
    "perm_exit_rate"
]

policy_vars = [
    "overall_days","s1_days","s1_CVD_days","s1_NP_days",
    "s2_days","s2_CVD_days","s2_NP_days",
    "s3_days","s3_CVD_days","s3_NP_days",
    "s4_days","s5_days","cares_days","cdc_days", "moratorium_intensity", "weighted_scorecard"
]

controls = ["U3", "COVID_cases"]

var_labels = {
    "inflow_rate": r"\shortstack{\\[0.1ex] Number of \\ First Time Homeless \\ Per Thousand in Population}",
    "median_days_homeless": r"\shortstack{\\[0.1ex] Median \\ Days Homeless}",
    "avg_days_homeless": r"\shortstack{\\[0.1ex] Average \\ Days Homeless}",
    "exit_rate": r"\shortstack{\\[0.1ex] Exits from Homelessness \\ Per Thousand in Population}",
    "perm_exit_rate": r"\shortstack{\\[0.1ex] Exits to Permanent Housing \\ Per Thousand in Population}",
    "U3": "Unemployment Rate",
    "COVID_cases": "COVID Cases",
    "moratorium_intensity": "Moratorium Intensity",
    "SCORECARD": "Scorecard",
    "weighted_scorecard": "Weighted Scorecard",
    "overall_days": "Overall Moratorium Days",
    "s1_days": "Stage 1 Days",
    "s1_CVD_days": "Stage 1 COVID Days",
    "s1_NP_days": "Stage 1 Non-Payment Days",
    "s2_days": "Stage 2 Days",
    "s2_CVD_days": "Stage 2 COVID Days",
    "s2_NP_days": "Stage 2 Non-Payment Days",
    "s3_days": "Stage 3 Days",
    "s3_CVD_days": "Stage 3 COVID Days",
    "s3_NP_days": "Stage 3 Non-Payment Days",
    "s4_days": "Stage 4 Days",
    "s5_days": "Stage 5 Days",
    "cares_days": "CARES Act Days",
    "cdc_days": "CDC Moratorium Days",
}

def run_models(df):

    df = df.copy()
    df = df.set_index(["state_code", "year"]).sort_index()

    # Create additional variables
    df['inflow_rate'] = df['inflow'] / df['POP'] * 100000
    df['exit_rate'] = df['exits'] / df['POP'] * 100000  
    df['perm_exit_rate'] = df['exits_perm'] / df['POP'] * 100000
    df['moratorium_intensity'] = df['overall_days'] * df['SCORECARD']
    df['weighted_scorecard'] = df['SCORECARD']
    df.loc[df['overall_days'] == 0, 'weighted_scorecard'] = 0

    for policy in policy_vars:
        results = {}
        within_r2 = {}
        obs = {}

        for y in outcomes:
            vars_needed = [y, policy] + controls
            df_clean = df[vars_needed].dropna()

            Y = df_clean[y]
            X = df_clean[[policy] + controls]

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

            col_entries = []

            for var in [policy] + controls:
                if var in res.params.index:
                    coef = res.params[var]
                    se = res.std_errors[var]
                    pval = res.pvalues[var]

                    coef_str = f"{coef:.3f}{star_format(pval)}"
                    se_str = f"({se:.3f})"
                else:
                    coef_str = "omitted"
                    se_str = ""

                col_entries.append(coef_str)
                col_entries.append(se_str)

            results[y] = col_entries
            within_r2[y] = f"{res.rsquared_within:.3f}"
            obs[y] = f"{int(res.nobs)}"

        # Build coefficient/se row index
        index_rows = []
        for var in [policy] + controls:
            index_rows.append(var_labels.get(var, var))
            index_rows.append("")

        table = pd.DataFrame(results, index=index_rows)

        # Add model summary rows
        table.loc["Within $R^2$"] = [within_r2[y] for y in outcomes]

        # Relabel outcome columns
        table.columns = [var_labels.get(col, col) for col in table.columns]

        outpath = TABLES / f"st_{policy}_results.tex"
        table.to_latex(outpath, escape=False)

        print(f"✓ Finished {policy}")


if __name__ == "__main__":
    df = pd.read_stata(ALL_STATE_DATA)
    run_models(df)