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


def run_models(df):
    df = df.copy()
    # Make sure panel index is set
    df = df.set_index(["state_code", "year"]).sort_index()

    for policy in POLICY_VARS:
        results = {}
        within_r2 = {}
        obs = {}

        for y in OUTCOMES:
            vars_needed = [y, policy] + CONTROLS
            df_clean = df[vars_needed].dropna()

            Y = df_clean[y]
            X = df_clean[[policy] + CONTROLS]

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

        # Build coefficient/se row index
        index_rows = []
        for var in [policy] + CONTROLS:
            index_rows.append(VAR_LABELS.get(var, var))
            index_rows.append("")

        table = pd.DataFrame(results, index=index_rows)

        # Add model summary rows
        table.loc["Within $R^2$"] = [within_r2[y] for y in OUTCOMES]

        # Relabel outcome columns
        table.columns = [VAR_LABELS.get(col, col) for col in table.columns]

        outpath = TABLES / f"st_{policy}_results.tex"
        table.to_latex(outpath, escape=False)

        print(f"✓ Finished {policy}")


if __name__ == "__main__":
    df = pd.read_stata(ALL_STATE_DATA)
    run_models(df)