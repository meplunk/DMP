from config import ALL_STATE_DATA, TABLES, OUTCOMES, POLICY_VARS, CONTROLS, VAR_LABELS, TO_SHARE
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


def prepare_data(df):
    df = df.copy()

    df["total_days"] = 365
    df.loc[df["year"] % 4 == 0, "total_days"] = 366

    # centered linear time variable
    df["t"] = df["year"] - df["year"].min()

    # share variables
    for var in TO_SHARE:
        df[f"share_{var}"] = df[var] / df["total_days"]

    # outcome variables
    df["inflow_rate"] = df["inflow"] / df["POP"] * 100000
    df["exit_rate"] = df["exits"] / df["POP"] * 100000
    df["perm_exit_rate"] = df["exits_perm"] / df["POP"] * 100000

    # intensity variables
    df["moratorium_intensity"] = df["overall_days"] * df["SCORECARD"]
    df["share_moratorium_intensity"] = df["share_overall_days"] * df["SCORECARD"]

    df["weighted_scorecard"] = df["SCORECARD"]
    df.loc[df["overall_days"] == 0, "weighted_scorecard"] = 0

    return df


def add_state_linear_trends(df):
    """
    Create manual state-specific linear trend regressors:
    trend_STATE = 1[state==STATE] * t

    One state is omitted as the reference group to avoid perfect collinearity.
    """
    df = df.copy()

    states = sorted(df["state_code"].dropna().unique())

    # omit first state as reference category
    trend_states = states[1:]

    trend_vars = []
    for state in trend_states:
        trend_name = f"trend_{state}"
        df[trend_name] = (df["state_code"] == state).astype(int) * df["t"]
        trend_vars.append(trend_name)

    return df, trend_vars


def run_models(df):
    df = prepare_data(df)
    df, trend_vars = add_state_linear_trends(df)

    # PanelOLS wants a MultiIndex
    df = df.set_index(["state_code", "year"]).sort_index()

    for policy in POLICY_VARS:
        results = {}
        within_r2 = {}
        obs = {}

        for y in OUTCOMES:
            vars_needed = [y, policy] + CONTROLS + trend_vars
            df_clean = df[vars_needed].dropna()

            Y = df_clean[y]
            X = df_clean[[policy] + CONTROLS + trend_vars]

            model = PanelOLS(
                Y,
                X,
                entity_effects=True,   # state FE
                time_effects=True,     # year FE
                drop_absorbed=True
            )

            res = model.fit(
                cov_type="clustered",
                cluster_entity=True
            )

            col_entries = []

            for var in [policy] + CONTROLS:
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

        # row labels for displayed coefficients only
        index_rows = []
        for var in [policy] + CONTROLS:
            index_rows.append(VAR_LABELS.get(var, var))
            index_rows.append("")

        table = pd.DataFrame(results, index=index_rows)

        table.loc["Within $R^2$"] = [within_r2[y] for y in OUTCOMES]
        table.loc["Observations"] = [obs[y] for y in OUTCOMES]
        table.loc["State FE"] = ["Yes"] * len(OUTCOMES)
        table.loc["Year FE"] = ["Yes"] * len(OUTCOMES)
        table.loc["State-specific linear trends"] = ["Yes"] * len(OUTCOMES)

        table.columns = [VAR_LABELS.get(col, col) for col in table.columns]

        outpath = TABLES / f"st_{policy}_results_state_trends.tex"
        table.to_latex(outpath, escape=False)

        print(f"✓ Finished {policy}")


if __name__ == "__main__":
    df = pd.read_stata(ALL_STATE_DATA)
    run_models(df)