from config import ALL_STATE_DATA, TABLES, GRAPHS
import pandas as pd
from linearmodels.panel import PanelOLS
from matplotlib import pyplot as plt
import seaborn as sns


def star_format(p):
    if p < 0.01:
        return "***"
    elif p < 0.05:
        return "**"
    elif p < 0.1:
        return "*"
    else:
        return ""


def pre_trend_graphs():
    df = pd.read_stata(ALL_STATE_DATA)

    # Create summary dataframe for plotting if ever_treated == 1
    df_summary_treated = df[df["ever_treated"] == 1].groupby("year").agg({
        'inflow_rate': 'mean',
        'exit_rate': 'mean',
        'avg_days_homeless': 'mean'
    }).reset_index()

    # Create summary dataframe for plotting if ever_treated == 0
    df_summary_untreated = df[df["ever_treated"] == 0].groupby("year").agg({
        'inflow_rate': 'mean',
        'exit_rate': 'mean',
        'avg_days_homeless': 'mean'
    }).reset_index()

    df_summary_treated.to_csv(GRAPHS / "pre_trends_treated.csv", index=False)
    df_summary_untreated.to_csv(GRAPHS / "pre_trends_untreated.csv", index=False)


    # Inflow plot with black line at year = 2019
    plt.figure(figsize=(12, 6))
    sns.lineplot(data=df_summary_treated, x='year', y='inflow_rate', marker='o', label='Ever Treated')
    sns.lineplot(data=df_summary_untreated, x='year', y='inflow_rate', marker='o', label='Never Treated')
    plt.axvline(x=2019, color='black', linestyle='--', linewidth=2)
    plt.title('Inflow to Homelessness Over Time')
    plt.xlabel('Year')
    plt.ylabel('First Time Homelessness per 100,000 Population')
    plt.legend()
    plt.tight_layout()
    plt.savefig(GRAPHS / "inflow_trends_state.png", dpi=300)
    plt.show()

    # Exits plot
    plt.figure(figsize=(12, 6))
    sns.lineplot(data=df_summary_treated, x='year', y='exit_rate', marker='o', label='Ever Treated')
    sns.lineplot(data=df_summary_untreated, x='year', y='exit_rate', marker='o', label='Never Treated')
    plt.axvline(x=2019, color='black', linestyle='--', linewidth=2)
    plt.title('Exits from Homelessness Over Time')
    plt.xlabel('Year')
    plt.ylabel('Exits per 100,000 Population')
    plt.legend()
    plt.tight_layout()
    plt.savefig(GRAPHS / "exits_trends_state.png", dpi=300)
    plt.show()

    # Average days homeless plot
    plt.figure(figsize=(12, 6))
    sns.lineplot(data=df_summary_treated, x='year', y='avg_days_homeless', marker='o', label='Ever Treated')
    sns.lineplot(data=df_summary_untreated, x='year', y='avg_days_homeless', marker='o', label='Never Treated')
    plt.axvline(x=2019, color='black', linestyle='--', linewidth=2)
    plt.title('Average Days Homeless Over Time')
    plt.xlabel('Year')
    plt.ylabel('Days Homeless')
    plt.legend()
    plt.tight_layout()
    plt.savefig(GRAPHS / "days_homeless_trends_state.png", dpi=300)
    plt.show()

def pre_trends_table():
    df = pd.read_stata(ALL_STATE_DATA).copy()

    # Ever treated = state has any active moratorium at any point in sample
    df["ever_treated"] = (
        (df["overall_days"] > 0)
        .groupby(df["state_code"])
        .transform("max")
        .astype(int)
    )

    # Outcomes
    df["inflow_rate"] = df["inflow"] / df["POP"] * 100000
    df["exit_rate"] = df["exits"] / df["POP"] * 100000

    # Restrict to pre-treatment period
    df = df[df["year"] < 2020].copy()

    # Linear time trend in pre-period
    # 2016->0, 2017->1, 2018->2, 2019->3
    df["pretrend"] = df["year"] - 2016
    df["treated_trend"] = df["ever_treated"] * df["pretrend"]

    # Set panel index
    df = df.set_index(["state_code", "year"]).sort_index()

    outcomes = {
        "inflow": "Inflow",
        "exits": "Exits",
        "median_days_homeless": "Median Days Homeless",
    }

    results = {}

    for outcome, label in outcomes.items():
        vars_needed = [outcome, "treated_trend"]
        df_clean = df[vars_needed].dropna()

        Y = df_clean[outcome]
        X = df_clean[["treated_trend"]]

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

        coef = res.params.get("treated_trend", float("nan"))
        se = res.std_errors.get("treated_trend", float("nan"))
        pval = res.pvalues.get("treated_trend", float("nan"))

        results[label] = [
            f"{coef:.3f}{star_format(pval)}" if pd.notna(coef) else "omitted",
            f"({se:.3f})" if pd.notna(se) else "",
            f"{int(res.nobs)}",
            "Yes",
            "Yes",
            f"{res.rsquared_within:.3f}",
        ]

    table = pd.DataFrame(
        results,
        index=[
            r"Ever Treated $\times$ Linear Time Trend",
            "",
            "Observations",
            "State FE",
            "Year FE",
            "Within $R^2$",
        ],
    )

    outpath = TABLES / "pre_trends_state.tex"
    table.to_latex(
        outpath,
        escape=False,
        index=True,
        column_format="lccc"
    )

    print(f"✓ Wrote {outpath}")  

    

def main():
    pre_trend_graphs()
    pre_trends_table()


if __name__ == "__main__":
    main()

