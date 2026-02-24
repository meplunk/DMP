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

def make_summary_table(df, varlist, filename):

    groups = {
        "Full Sample (N = {})".format(len(df)): df,
        "Control (N = {})".format(len(df[df["overall_days"] == 0])): df[df["overall_days"] == 0],
        "Treatment (N = {})".format(len(df[df["overall_days"] != 0])): df[df["overall_days"] != 0]
    }

    panels = []

    for title, data in groups.items():

        summary = data[varlist].agg(["mean", "std", "min", "max"]).T

        summary["mean"] = summary["mean"].round(3)
        summary["std"]  = summary["std"].round(3)

        for var in varlist:
            if var in ["POP", "COVID_cases", "COVID_deaths"]:
                summary.loc[var, "min"] = round(summary.loc[var, "min"])
                summary.loc[var, "max"] = round(summary.loc[var, "max"])
            else:
                summary.loc[var, "min"] = round(summary.loc[var, "min"], 3)
                summary.loc[var, "max"] = round(summary.loc[var, "max"], 3)

        summary.index = summary.index.map(var_labels)

        # Rename columns
        summary.columns = pd.MultiIndex.from_product([[title], ["Mean", "SD", "Min", "Max"]])

        panels.append(summary)

    final_table = pd.concat(panels, axis=1)

    # ------------------------------------------------------------
    # Convert to string with custom formatting
    # ------------------------------------------------------------

    for col in final_table.columns:
        stat = col[1]  # Mean, SD, Min, Max

        if stat in ["Mean", "SD"]:
            final_table[col] = final_table[col].map(lambda x: f"{float(x):.3f}")

        elif stat in ["Min", "Max"]:
            for idx in final_table.index:
                if idx in count_variables:
                    final_table.loc[idx, col] = f"{int(round(float(final_table.loc[idx, col])))}"
                else:
                    final_table.loc[idx, col] = f"{float(final_table.loc[idx, col]):.3f}"

    # ------------------------------------------------------------
    # Export to LaTeX
    # ------------------------------------------------------------

    latex = final_table.to_latex(
        multicolumn=True,
        multicolumn_format="c",
        escape=False
    )

    with open(TABLES / filename, "w") as f:
        f.write(latex)

    print(f"✓ Saved {TABLES / filename}")

def main():
    print("Loading data...")
    df = pd.read_stata(ALL_DATA)
    print("Making summary tables...")
    make_summary_table(df, outcomes, "outcomes_summary.tex")
    make_summary_table(df, covariates, "covariates_summary.tex")

if __name__ == "__main__":
    main()