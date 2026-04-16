from config import ALL_STATE_DATA, TABLES, OUTCOMES, POLICY_VARS, TO_SHARE, CONTROLS, VAR_LABELS
import pandas as pd
import statsmodels.formula.api as smf
from wildboottest.wildboottest import wildboottest


def star_format(p):
    if p < 0.01:
        return "***"
    elif p < 0.05:
        return "**"
    elif p < 0.1:
        return "*"
    else:
        return ""


def run_models(df, use_state_trends=False, bootstrap_reps=9999, bootstrap_type="31"):

    for policy in POLICY_VARS:
        results = {}
        r2 = {}
        obs = {}
        wild_pvals = {}

        for y in OUTCOMES:
            vars_needed = [y, policy] + CONTROLS + ["state_code", "year"]
            if use_state_trends:
                vars_needed.append("t")

            df_clean = df[vars_needed].dropna().copy()

            # make numeric cluster ids
            df_clean["state_cluster"] = pd.Categorical(df_clean["state_code"]).codes.astype("int64")

            rhs_terms = [policy] + CONTROLS + ["C(state_code)", "C(year)"]
            if use_state_trends:
                rhs_terms.append("C(state_code):t")

            formula = f"{y} ~ " + " + ".join(rhs_terms)

            model = smf.ols(formula, data=df_clean)

            res = model.fit(
                cov_type="cluster",
                cov_kwds={"groups": df_clean["state_cluster"]}
            )

            wb = wildboottest(
                model,
                param=policy,
                cluster=df_clean["state_cluster"].to_numpy(),
                B=bootstrap_reps,
                bootstrap_type=bootstrap_type
            )

            wb_pval = float(wb["p-value"].iloc[0])
            wild_pvals[y] = wb_pval

            col_entries = []

            for var in [policy] + CONTROLS:
                if var in res.params.index:
                    coef = res.params[var]
                    se = res.bse[var]

                    if var == policy:
                        pval_for_stars = wb_pval
                    else:
                        pval_for_stars = res.pvalues[var]

                    coef_str = f"{coef:.3f}{star_format(pval_for_stars)}"
                    se_str = f"({se:.3f})"
                else:
                    coef_str = "omitted"
                    se_str = ""

                col_entries.append(coef_str)
                col_entries.append(se_str)

            results[y] = col_entries
            r2[y] = f"{res.rsquared:.3f}"
            obs[y] = f"{int(res.nobs)}"

        index_rows = []
        for var in [policy] + CONTROLS:
            index_rows.append(VAR_LABELS.get(var, var))
            index_rows.append("")

        table = pd.DataFrame(results, index=index_rows)

        table.loc["$R^2$"] = [r2[y] for y in OUTCOMES]
        table.loc["Observations"] = [obs[y] for y in OUTCOMES]
        table.loc["State FE"] = ["Yes"] * len(OUTCOMES)
        table.loc["Year FE"] = ["Yes"] * len(OUTCOMES)
        table.loc["State-specific linear trends"] = [
            "Yes" if use_state_trends else "No"
        ] * len(OUTCOMES)
        table.loc["SEs clustered by state"] = ["Yes"] * len(OUTCOMES)
        table.loc["Wild cluster bootstrap p-val"] = [
            f"{wild_pvals[y]:.3f}" for y in OUTCOMES
        ]

        table.columns = [VAR_LABELS.get(col, col) for col in table.columns]

        suffix = "_state_trends" if use_state_trends else ""
        outpath = TABLES / f"st_{policy}_results_wildbootstrap{suffix}.tex"
        table.to_latex(outpath, escape=False)

        print(f"✓ Finished {policy}")


if __name__ == "__main__":
    df = pd.read_stata(ALL_STATE_DATA)
    run_models(df, use_state_trends=False)