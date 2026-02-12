from pathlib import Path
import pandas as pd
from config import HUD_DATA, HUD_CLEAN, COVARIATES, POLICY_PANEL, ALL_DATA


# ------------------------------------------------------------
# 1. Load HUD Data (already naturally long)
# ------------------------------------------------------------

def load_HUD_data(HUD_path):
    xls = pd.ExcelFile(HUD_path)

    df_list = []
    sheets = xls.sheet_names[1:]

    for sheet in sheets:
        try:
            year = int(sheet)
        except ValueError:
            print(f"Skipping non-year sheet: {sheet}")
            continue

        temp = pd.read_excel(HUD_path, sheet_name=sheet, header=1)
        temp["year"] = year
        df_list.append(temp)

    df_combined = pd.concat(df_list, ignore_index=True)

    return df_combined


# ------------------------------------------------------------
# 2. Clean HUD Data (keep LONG)
# ------------------------------------------------------------

def clean_HUD_data(df):

    rename_map = {
        "Continuum of Care (CoC)": "coc_name",
        "HUD CoC Number": "coc_code",
        "ES-SH-TH-PH 1st Time Homeless": "inflow",
        "ES-SH-TH Avg (Days)": "avg_days_homeless",
        "ES-SH-TH Median (Days)": "median_days_homeless",
        "Percent with Successful  ES, TH, SH, PH-RRH Exit": "success_rate",
        "Total Persons Exiting ES, TH, SH, PH-RRH": "exits",
        "Total Persons Exiting ES, TH, SH, PH-RRH to Permanent Housing": "exits_perm",
        "Total Non-DV Beds on 2015 HIC ES+TH": "beds_2015",
        "2015 Bed coverage Percent on HMIS for ES-TH Combined": "bed_coverage_pct"
    }

    df = df.rename(columns=lambda x: x.strip() if isinstance(x, str) else x)
    df = df.rename(columns=rename_map)

    keep_cols = [
        "coc_name", "coc_code", "year", "inflow",
        "avg_days_homeless", "median_days_homeless",
        "success_rate", "exits", "exits_perm",
        "beds_2015", "bed_coverage_pct"
    ]

    df = df[[c for c in keep_cols if c in df.columns]]

    df = df.dropna(subset=["coc_code"])

    # Ensure numeric
    num_cols = [
        "inflow", "avg_days_homeless", "median_days_homeless",
        "exits", "exits_perm", "success_rate"
    ]

    df[num_cols] = df[num_cols].apply(pd.to_numeric, errors="coerce")

    # Optional: drop CoCs missing years
    years_per_coc = df.groupby("coc_code")["year"].count()
    missings = years_per_coc[years_per_coc < df["year"].nunique()]

    if not missings.empty:
        print("[WARNING] Dropping CoCs with missing years:")
        print(missings)
        df = df[~df["coc_code"].isin(missings.index)]

    # Save long cleaned HUD
    df.to_csv(HUD_CLEAN, index=False)

    return df


# ------------------------------------------------------------
# 3. Merge Covariates (already long)
# ------------------------------------------------------------

def merge_covariates(df_hud):

    covars = pd.read_csv(COVARIATES)

    # Expecting:
    # coc_id | year | POP | UNEMP | COVID...

    df = pd.merge(
        df_hud,
        covars,
        left_on=["coc_code", "year"],
        right_on=["coc_id", "year"],
        how="left"
    )

    return df


# ------------------------------------------------------------
# 4. Merge State Policy Panel
# ------------------------------------------------------------

def merge_policy(df):

    policy = pd.read_csv(POLICY_PANEL)

    # Extract state postal code from HUD CoC code
    df["state_code"] = df["coc_code"].str.split("-").str[0].str.strip()

    # Merge policy panel
    df = pd.merge(
        df,
        policy,
        on=["state_code", "year"],
        how="left"
    )

    # ------------------------------------------------------------
    # Fill policy columns with zeros outside 2020–2022
    # ------------------------------------------------------------

    policy_cols = [col for col in policy.columns 
                   if col not in ["state_code", "year"]]

    df[policy_cols] = df[policy_cols].fillna(0)

    df = df.drop(columns=["state_code", "coc_id"])  # Drop helper columns

    return df



# ------------------------------------------------------------
# MAIN
# ------------------------------------------------------------

def main():
    print("Loading HUD data...")
    df = load_HUD_data(HUD_DATA)

    print("Cleaning HUD data (long format)...")
    df = clean_HUD_data(df)

    print("Merging CoC covariates...")
    df = merge_covariates(df)

    print("Merging state policy data...")
    df = merge_policy(df)

    print("Saving final merged dataset...")
    df.to_csv(ALL_DATA, index=False)

    print("Done.")
    print(df.head())


if __name__ == "__main__":
    main()
