from config import ALL_STATE_DATA, GRAPHS, TABLES
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns   

def plot_homelessness_flows():
    # Load the data
    df = pd.read_stata(ALL_STATE_DATA)

    # Create rate vars
    df['inflow_rate'] = df['inflow'] / df['POP'] * 100000
    df['exit_rate'] = df['exits'] / df['POP'] * 100000

    # Filter for the relevant columns
    df = df[['year', 'state_code', 'inflow_rate', 'exit_rate', 'avg_days_homeless', 'median_days_homeless']]

    # Collapse by year (state averages)
    df_summary = df.groupby('year').agg({
        'inflow_rate': 'mean',
        'exit_rate': 'mean',
        'avg_days_homeless': 'mean',
        'median_days_homeless': 'mean'
    }).reset_index()

    # Plot line graphs for inflow and exits with labels
    plt.figure(figsize=(6, 6))
    sns.lineplot(data=df_summary, x='year', y='inflow_rate', marker='o', label='Inflow')
    sns.lineplot(data=df_summary, x='year', y='exit_rate', marker='o', label='Exits')
    plt.title('Average Inflow and Exits from Homelessness Over Time')
    plt.xlabel('Year')
    plt.ylabel('Number of People per Thousand Population')
    plt.legend()
    plt.tight_layout()
    plt.savefig(GRAPHS / "homelessness_flows.png", dpi=300)
    plt.show()

    # Plot line graphs for average and median days homeless with labels
    plt.figure(figsize=(6, 6)) 
    sns.lineplot(data=df_summary, x='year', y='avg_days_homeless', marker='o', label='Average Days Homeless')
    sns.lineplot(data=df_summary, x='year', y='median_days_homeless', marker='o', label='Median Days Homeless')
    plt.title('Average and Median Days Homeless Over Time')
    plt.xlabel('Year')
    plt.ylabel('Days Homeless')
    plt.legend()
    plt.tight_layout()
    plt.savefig(GRAPHS / "days_homeless.png", dpi=300)
    plt.show()


outcomes = [
    "inflow_rate",
    "avg_days_homeless",
    "exit_rate",
]

covariates = [
    "POP",
    "RENT_POP",
    "U3",
    "COVID_cases",
]

var_labels = {
    "inflow_rate": "First Time Homeless per Thousand Population",
    "avg_days_homeless": "Average Days Homeless",
    "exit_rate": "Exits from Homelessness per Thousand Population",
    "POP": "Population",
    'RENT_POP': "Renting Population",
    "U3": "Unemployment Rate",
    "COVID_cases": "COVID Cases",
}

count_variables = {
    "First Time Homeless per Thousand Population",
    "Average Days Homeless",
    "Exits from Homelessness per Thousand Population",
    "Population",
    "Renting Population",
    "COVID Cases",
}

def make_summary_table(df: pd.DataFrame, varlist: list[str], filename: str):

    df_use = df.copy()

    # Create inflow and exit rates
    df_use['inflow_rate'] = df_use['inflow'] / df_use['POP'] * 100000
    df_use['exit_rate'] = df_use['exits'] / df_use['POP'] * 100000


    groups = {
        f"Full Sample (N = {len(df_use)})": df_use,
        f"Control (N = {len(df_use[df_use['ever_treated'] == 0])})":
            df_use[df_use['ever_treated'] == 0],
        f"Treatment (N = {len(df_use[df_use['ever_treated'] == 1])})":
            df_use[df_use["ever_treated"] == 1],
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
    final_table = final_table.astype(object)
    for col in final_table.columns:
        stat = col[1]

        if stat in ["Mean", "SD"]:
            final_table[col] = final_table[col].map(lambda x: f"{float(x):.3f}")

        elif stat in ["Min", "Max"]:
            for idx in final_table.index:
                val = final_table.at[idx, col]
                if pd.isna(val):
                    final_table.at[idx, col] = ""
                    continue

                if idx in count_variables:
                    final_table.at[idx, col] = f"{int(round(float(val)))}"
                else:
                    final_table.at[idx, col] = f"{float(val):.3f}"

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
    df = pd.read_stata(ALL_STATE_DATA)

    if "year" not in df.columns:
        raise KeyError("Expected a 'year' column in ALL_DATA.")

    df_full   = df[(df["year"] >= 2016) & (df["year"] <= 2023)].copy()
    df_pre    = df[(df["year"] >= 2016) & (df["year"] <= 2019)].copy()
    df_covid  = df[(df["year"] >= 2020) & (df["year"] <= 2023)].copy()

    print("Making 6 tables...")

    # Outcomes
    make_summary_table(df_full, outcomes, "st_outcomes_summary_full_2016_2023.tex")
    make_summary_table(df_pre, outcomes, "st_outcomes_summary_pre_2016_2019.tex")
    make_summary_table(df_covid, outcomes, "st_outcomes_summary_covid_2020_2023.tex")

    # Covariates
    make_summary_table(df_full, covariates, "st_covariates_summary_full_2016_2023.tex")
    make_summary_table(df_pre, covariates, "st_covariates_summary_pre_2016_2019.tex") 
    make_summary_table(df_covid, covariates, "st_covariates_summary_covid_2020_2023.tex")

if __name__ == "__main__":
    main()


