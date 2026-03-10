from pathlib import Path
import pandas as pd
from config import HUD_DATA, HUD_CLEAN, STATE_CLEAN, POLICY_PANEL, ALL_STATE_DATA


state_fips_to_postal_code = {
    "1": "AL", "2": "AK", "4": "AZ", "5": "AR", "6": "CA", "8": "CO",
    "9": "CT", "10": "DE", "11": "DC", "12": "FL", "13": "GA", "15": "HI", "16": "ID",
    "17": "IL", "18": "IN", "19": "IA", "20": "KS", "21": "KY", "22": "LA", "23": "ME",
    "24": "MD", "25": "MA", "26": "MI", "27": "MN", "28": "MS", "29": "MO", "30": "MT", 
    "31": "NE", "32": "NV", "33": "NH", "34": "NJ", "35": "NM", "36": "NY", "37": "NC", 
    "38": "ND", "39": "OH", "40": "OK", "41": "OR", "42": "PA", "44": "RI", "45": "SC", 
    "46": "SD", "47": "TN", "48": "TX", "49": "UT", "50": "VT", "51": "VA", "53": "WA", 
    "54": "WV", "55": "WI", "56": "WY", "72": "PR", "78": "VI"}

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

    # Erroneous data: NY-511 in 2016 had a negative sucess rate, drop this obs
    df = df[~((df["coc_code"] == "NY-511") & (df["year"] == 2016))]

    df[num_cols] = df[num_cols].apply(pd.to_numeric, errors="coerce")

    # Optional: drop CoCs missing years
    years_per_coc = df.groupby("coc_code")["year"].count()
    missings = years_per_coc[years_per_coc < df["year"].nunique()]

    if not missings.empty:
        print("[WARNING] Dropping CoCs with missing years:")
        print(missings)
        df = df[~df["coc_code"].isin(missings.index)]

    # create state variable from coc_cide first two letters
    df["state"] = df["coc_code"].str[:2]

    # Drop coc_code 
    df = df.drop(columns=["coc_code", "success_rate"])

    # Collapse to state-year level by taking population-weighted average of success_rate and simple average of other variables
    df = df.groupby(["state", "year"]).agg({
        "inflow": "sum",
        "avg_days_homeless": "mean",
        "median_days_homeless": "mean",
        "exits": "sum",
        "exits_perm": "sum",
    }).reset_index()

    return df


# ------------------------------------------------------------
# 3. Merge Covariates (already long)
# ------------------------------------------------------------

def merge_covariates(df_hud):

    covars = pd.read_csv(STATE_CLEAN)

    # adding a postal code column to state covars
    covars["state_code"] = covars["state_fips"].astype(str).map(state_fips_to_postal_code)

    # drop state_fips and state
    covars = covars.drop(columns=["state_fips", "state"])
    
    df = pd.merge(
        df_hud,
        covars,
        left_on=["state", "year"],
        right_on=["state_code", "year"],
        how="left"
    )

    print("Merged HUD + covariates head:"
          , df.head())
    
    return df


# ------------------------------------------------------------
# 4. Merge State Policy Panel
# ------------------------------------------------------------

def merge_policy(df):

    policy = pd.read_csv(POLICY_PANEL)

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

    policy_cols = [
    'state_fips', 'overall_days', 's1_days', 's1_CVD_days', 's1_NP_days',
    's2_days', 's2_CVD_days', 's2_NP_days', 's3_days', 's3_CVD_days',
    's3_NP_days', 's4_days', 's5_days', 'cares_days', 'cdc_days',
    'total_days', 'RENT_POP', 'SCORECARD'
    ]   

    # fill RENT_POP and SCORECARD, should be the same in all years for a given state
    df[["RENT_POP", "SCORECARD"]] = (
    df.sort_values(["state_code", "year"])
      .groupby("state_code")[["RENT_POP", "SCORECARD"]]
      .ffill()
      .bfill()
    )

    df[policy_cols] = df[policy_cols].fillna(0)

    # drop total days, state_x, state_y, move state_code to front
    df = df.drop(columns=["total_days", "state_x", "state_y"])
    df = df[["state_code"] + [col for col in df.columns if col != "state_code"]]

    # Dropping obs with POP == 0 (only 7 obs, 5 of which are islands which are outliers in other ways)
    df = df[df["POP"] > 0]

    # ------------------------------------------------------------
    # Create ever-treated indicator at CoC level
    # ------------------------------------------------------------

    ever_treated = (
        df.groupby("state_code")["overall_days"]
        .max()
        .reset_index()
        .rename(columns={"overall_days": "ever_treated"})
    )

    # ever_treated = 1 if moratorium ever occurs in that CoC
    ever_treated["ever_treated"] = (ever_treated["ever_treated"] > 0).astype(int)

    # Merge back to full panel
    df = df.merge(ever_treated, on="state_code", how="left")
    
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
    df.to_stata(ALL_STATE_DATA)

    print("Done.")
    print(df.head())


if __name__ == "__main__":
    main()
