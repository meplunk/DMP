from config import ALL_DATA, TABLES
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
    "inflow",
    "avg_days_homeless",
    "median_days_homeless",
    "success_rate",
    "exits",
    "exits_perm"
]

policy_vars = [
    "overall_days","s1_days","s1_CVD_days","s1_NP_days",
    "s2_days","s2_CVD_days","s2_NP_days",
    "s3_days","s3_CVD_days","s3_NP_days",
    "s4_days","s5_days","cares_days","cdc_days","total_days"
]

controls = ["POP", "UNEMP", "COVID_cases"]

var_labels = {
    "inflow": r"\shortstack{\\[0.1ex] Number of \\ First Time \\ Homeless}",
    "avg_days_homeless": r"\shortstack{\\[0.1ex] Average \\ Days Homeless}",
    "median_days_homeless": r"\shortstack{\\[0.1ex] Median \\ Days Homeless}",
    "success_rate": r"\shortstack{\\[0.1ex] Percent with \\ Successful Exit \\ to Permanent Housing}",
    "exits": "Exits",
    "exits_perm": r"\shortstack{\\[0.1ex] Number of \\ Exits to \\ Permanent Housing}",
    "POP": "Population",
    "UNEMP": "Unemployment Rate",
    "COVID_cases": "COVID Cases",
    "COVID_deaths": "COVID Deaths",
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
    "total_days": "Total Moratorium Days"
}

def run_models(df):

    df = df.copy()
    df = df.set_index(["coc_code", "year"]).sort_index()

    for policy in policy_vars:

        results = {}

        for y in outcomes:

            vars_needed = [y, policy, "POP", "UNEMP", "COVID_cases"]
            df_clean = df[vars_needed].dropna()

            Y = df_clean[y]
            X = df_clean[[policy, "POP", "UNEMP", "COVID_cases"]]

            model = PanelOLS(
                Y,
                X,
                entity_effects=True,
                time_effects=True
            )

            res = model.fit(
                cov_type="clustered",
                cluster_entity=True
            )

            col_entries = []

            for var in [policy, "POP", "UNEMP", "COVID_cases"]:

                coef = res.params[var]
                se   = res.std_errors[var]
                pval = res.pvalues[var]

                stars = star_format(pval)

                coef_str = f"{coef:.3f}{stars}"
                se_str   = f"({se:.3f})"

                col_entries.append(coef_str)
                col_entries.append(se_str)

            results[y] = col_entries

        # Build table index
        index_rows = []
        for var in [policy, "POP", "UNEMP", "COVID_cases"]:
            label = var_labels.get(var, var)
            index_rows.append(label)
            index_rows.append("")

        table = pd.DataFrame(results, index=index_rows)

        # Relabel outcome columns
        table.columns = [var_labels.get(col, col) for col in table.columns]

        outpath = TABLES / f"{policy}_results.tex"
        table.to_latex(outpath, escape=False)

        print(f"✓ Finished {policy}")

if __name__ == "__main__":
    df = pd.read_stata(ALL_DATA)
    run_models(df)