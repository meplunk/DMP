from pathlib import Path
import pandas as pd
import matplotlib.pyplot as plt
import statsmodels.api as sm
from config import HUD_DATA, HUD_CLEAN, COVARIATES, ALL_DATA


def load_HUD_data(HUD_path):
    # Read all sheet names
    xls = pd.ExcelFile(HUD_path)

    # Read and combine all sheets
    df_list = []
    sheets = xls.sheet_names[1:]
    for sheet in sheets:
        try:
            # Try to convert the sheet name to an integer year
            year = int(sheet)
        except ValueError:
            # If it’s not a number, skip or handle differently
            print(f"Skipping non-year sheet: {sheet}")
            continue

        temp = pd.read_excel(HUD_path, sheet_name=sheet, header=1)
        temp["year"] = year
        df_list.append(temp)

    # Combine all sheets
    df_combined = pd.concat(df_list, ignore_index=True)

    return df_combined

def clean_HUD_data(df):
    """
    Rename key HUD SPM variables to shorter, intuitive names.
    Expects exact column names (or very close) as in the HUD SPM export.
    """

    rename_map = {
        # Identifiers
        "Continuum of Care (CoC)": "coc_name",
        "HUD CoC Number": "coc_code",
        "year": "year",
        "ES-SH-TH-PH 1st Time Homeless" : "inflow",

        # Length of time homeless (congestion)
        "ES-SH-TH Avg (Days)": "avg_days_homeless",
        "ES-SH-TH Median (Days)": "median_days_homeless",

        # Rehousing throughput
        "Percent with Successful  ES, TH, SH, PH-RRH Exit": "success_rate",
        "Total Persons Exiting ES, TH, SH, PH-RRH": "exits",
        "Total Persons Exiting ES, TH, SH, PH-RRH to Permanent Housing": "exits_perm",

        # Controls
        "Total Non-DV Beds on 2015 HIC ES+TH": "beds_2015",
        "2015 Bed coverage Percent on HMIS for ES-TH Combined": "bed_coverage_pct"
    }

    # Apply the mapping
    df = df.rename(columns=lambda x: x.strip() if isinstance(x, str) else x)
    df = df.rename(columns=rename_map)

    # Keep only the renamed (and relevant) columns
    keep_cols = [
        "coc_name", "coc_code", "year",  "inflow",
        "avg_days_homeless", "median_days_homeless", 
        "success_rate", "exits", "exits_perm",
        "beds_2015", "bed_coverage_pct"
    ]
    df = df[[c for c in keep_cols if c in df.columns]]

    # Drop the two blank CoC rows
    df = df.dropna(subset=["coc_code"])

    # Ensure numeric columns are numeric
    num_cols = ["avg_days_homeless", "median_days_homeless",
                "exits", "exits_perm", "inflow"]
    df[num_cols] = df[num_cols].apply(pd.to_numeric, errors="coerce")

    years_per_coc = df.groupby('coc_code')['year'].count()
    missings = years_per_coc[years_per_coc < df['year'].nunique()]

    if not missings.empty:
        print("[WARNING] Some CoCs have missing years of data:")
        print(missings)
        drop_cocs = missings.index.tolist()
        df = df[~df['coc_code'].isin(drop_cocs)]

    df_wide = df.pivot(index='coc_code', 
                    columns='year', 
                    values=['coc_name', 'inflow', 'avg_days_homeless', 'median_days_homeless', 
                            'success_rate', 'exits', 'exits_perm'])

    # Flatten the multi-level column names
    df_wide.columns = [f'{col}_{year}' for col, year in df_wide.columns]
    df_wide = df_wide.reset_index()

    # Keep only the first coc_name column (they should all be the same)
    coc_name_cols = [col for col in df_wide.columns if col.startswith('coc_name_')]
    df_wide['coc_name'] = df_wide[coc_name_cols[0]]
    df_wide = df_wide.drop(columns=coc_name_cols)

    # Reorder to put coc_name second
    cols = ['coc_code', 'coc_name'] + [col for col in df_wide.columns if col not in ['coc_code', 'coc_name']]
    df_wide = df_wide[cols]

    # After reshape
    print(df_wide.shape)

    df_wide.to_csv(HUD_CLEAN, index=False)

    COVARIATE_DATA = pd.read_csv(COVARIATES)
    df_wide = pd.merge(df_wide, COVARIATE_DATA, left_on="coc_code", right_on="coc_id", how='left')

    return df_wide

def main():
    print("Loading HUD data...")
    df = load_HUD_data(HUD_DATA)
    print("Cleaning HUD data...")
    df = clean_HUD_data(df)
    print("Saving cleaned HUD data...")
    df.to_csv(ALL_DATA, index=False)

if __name__ == "__main__":
    main()