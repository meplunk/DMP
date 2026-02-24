from config import ALL_DATA, TABLES
import pandas as pd

outcomes = [
    "inflow",
    "avg_days_homeless",
    "median_days_homeless",
    "success_rate",
    "exits",
    "exits_perm"
]

covariates = [
    "POP",
    "UNEMP",
    "COVID_cases",
    "COVID_deaths"
]

var_labels = {
    "inflow": "Number of First Time Homeless",
    "avg_days_homeless": "Average Days Homeless",
    "median_days_homeless": "Median Days Homeless",
    "success_rate": "Percent with Successful Exit",
    "exits": "Exits",
    "exits_perm": "Exits to Permanent Housing",
    "POP": "Population",
    "UNEMP": "Unemployment Rate",
    "COVID_cases": "COVID Cases",
    "COVID_deaths": "COVID Deaths"
}

count_variables = {
    "Number of First Time Homeless",
    "Average Days Homeless",
    "Median Days Homeless",
    "Exits",
    "Exits to Permanent Housing",
    "Population",
    "COVID Cases",
    "COVID Deaths"
}

def make_summary_table(df: pd.DataFrame, varlist: list[str], filename: str, split: bool = True):

    needed = ["overall_days"] + varlist if split else varlist
    missing = [c for c in needed if c not in df.columns]
    if missing:
        raise KeyError(f"Missing columns needed for summary table: {missing}")

    df_use = df.copy()

    if split:
        groups = {
            f"Full Sample (N = {len(df_use)})": df_use,
            f"Control (N = {len(df_use[df_use['ever_treated'] == 0])})":
                df_use[df_use['ever_treated'] == 0],
            f"Treatment (N = {len(df_use[df_use['ever_treated'] == 1])})":
                df_use[df_use["ever_treated"] == 1],
        }
    else:
        groups = {
            f"Full Sample (N = {len(df_use)})": df_use
        }

    panels = []

    for title, data in groups.items():
        summary = data[varlist].agg(["mean", "std", "min", "max"]).T

        summary["mean"] = summary["mean"].round(3)
        summary["std"]  = summary["std"].round(3)

        summary.index = summary.index.map(var_labels)
        summary.columns = pd.MultiIndex.from_product([[title], ["Mean", "SD", "Min", "Max"]])

        panels.append(summary)

    final_table = pd.concat(panels, axis=1)

    # ------------------------------------------------------------
    # Custom formatting
    # ------------------------------------------------------------
    for col in final_table.columns:
        stat = col[1]

        if stat in ["Mean", "SD"]:
            final_table[col] = final_table[col].map(lambda x: f"{float(x):.3f}")

        elif stat in ["Min", "Max"]:
            for idx in final_table.index:
                val = final_table.loc[idx, col]
                if pd.isna(val):
                    final_table.loc[idx, col] = ""
                    continue

                if idx in count_variables:
                    final_table.loc[idx, col] = f"{int(round(float(val)))}"
                else:
                    final_table.loc[idx, col] = f"{float(val):.3f}"

    latex = final_table.to_latex(
        multicolumn=True,
        multicolumn_format="c",
        escape=False,
        index=True,
    )

    outpath = TABLES / filename
    outpath.parent.mkdir(parents=True, exist_ok=True)
    outpath.write_text(latex)

    print(f"✓ Saved {outpath}")


def main():
    print("Loading data...")
    df = pd.read_stata(ALL_DATA)

    if "year" not in df.columns:
        raise KeyError("Expected a 'year' column in ALL_DATA.")

    df_full   = df[(df["year"] >= 2016) & (df["year"] <= 2023)].copy()
    df_pre    = df[(df["year"] >= 2016) & (df["year"] <= 2019)].copy()
    df_covid  = df[(df["year"] >= 2020) & (df["year"] <= 2023)].copy()

    print("Making 6 tables...")

    # Outcomes
    make_summary_table(df_full, outcomes,
                       "outcomes_summary_full_2016_2023.tex",
                       split=True)

    make_summary_table(df_pre, outcomes,
                       "outcomes_summary_pre_2016_2019.tex",
                       split=True)  

    make_summary_table(df_covid, outcomes,
                       "outcomes_summary_covid_2020_2023.tex",
                       split=True)

    # Covariates
    make_summary_table(df_full, covariates,
                       "covariates_summary_full_2016_2023.tex",
                       split=True)

    make_summary_table(df_pre, covariates,
                       "covariates_summary_pre_2016_2019.tex",
                       split=True) 
    
    make_summary_table(df_covid, covariates,
                       "covariates_summary_covid_2020_2023.tex",
                       split=True)


if __name__ == "__main__":
    main()