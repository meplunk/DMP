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
    "overall_days","s1_days",
    "s2_days",
    "s3_days",
    "s4_days","s5_days"
]

policy_vars_detailed = [
    "overall_days","s1_days","s1_CVD_days","s1_NP_days",
    "s2_days","s2_CVD_days","s2_NP_days",
    "s3_days","s3_CVD_days","s3_NP_days",
    "s4_days","s5_days","cares_days","cdc_days","total_days"
]

controls = ["POP", "UNEMP"]

controls_with_covid = ["POP", "UNEMP", "COVID_cases"]

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
    "moratorium_index": "Moratorium Index",
}

def run_models(df):

    df = df.copy()
    df = df.set_index(["coc_code", "year"]).sort_index()

    results = {}

    for var in policy_vars:
        mean = df[var].mean()
        std  = df[var].std()

        df[f"{var}_std"] = (df[var] - mean) / std

    # Equal-weight average of standardized components
    std_vars = [f"{var}_std" for var in policy_vars]

    df["moratorium_index"] = df[std_vars].mean(axis=1)

    for y in outcomes:

        vars_needed = [y, "moratorium_index", "POP", "UNEMP", "COVID_cases"]
        df_clean = df[vars_needed].dropna()

        Y = df_clean[y]
        X = df_clean[["moratorium_index", "POP", "UNEMP"]]

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

        for var in ["moratorium_index", "POP", "UNEMP"]:

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
    for var in ["moratorium_index", "POP", "UNEMP"]:
        label = var_labels.get(var, var)
        index_rows.append(label)
        index_rows.append("")

    table = pd.DataFrame(results, index=index_rows)

    # Relabel outcome columns
    table.columns = [var_labels.get(col, col) for col in table.columns]

    outpath = TABLES / "moratorium_index_results.tex"
    table.to_latex(outpath, escape=False)

    print(f"✓ Finished moratorium_index analysis")

if __name__ == "__main__":
    df = pd.read_stata(ALL_DATA)
    run_models(df)