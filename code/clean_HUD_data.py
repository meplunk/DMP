from pathlib import Path
import pandas as pd
import matplotlib.pyplot as plt
import statsmodels.api as sm

def get_project_paths():
    """Return dictionary of all project paths"""
    root = Path(__file__).resolve().parent.parent
    return {
        # Raw Data
        'raw_data': root / "data/raw",
        'clean_data': root / 'data/clean'
    }

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

def rename_HUD_columns(df, PEL_path):
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
        "Percent with Successful  ES, TH, SH, PH-RRH Exit": "rehousing_success",
        "Total Persons Exiting ES, TH, SH, PH-RRH": "total_exits",
        "Total Persons Exiting ES, TH, SH, PH-RRH to Permanent Housing": "exits_perm_housing",

        # Controls
        "Total Non-DV Beds on 2015 HIC ES+TH": "beds_2015",
        "2015 Bed coverage Percent on HMIS for ES-TH Combined": "bed_coverage_pct"
    }

    # Apply the mapping
    df = df.rename(columns=lambda x: x.strip() if isinstance(x, str) else x)
    df = df.rename(columns=rename_map)

    # Keep only the renamed (and relevant) columns
    keep_cols = [
        "coc_name", "coc_code", "year",
        "avg_days_homeless", "median_days_homeless",
        "rehousing_success", "total_exits", "exits_perm_housing",
        "beds_2015", "bed_coverage_pct", "inflow"
    ]
    df = df[[c for c in keep_cols if c in df.columns]]

    # Drop the two blank CoC rows
    df = df.dropna(subset=["coc_code"])

    # Ensure numeric columns are numeric
    num_cols = ["avg_days_homeless", "median_days_homeless",
                "total_exits", "exits_perm_housing"]
    df[num_cols] = df[num_cols].apply(pd.to_numeric, errors="coerce")

    df['state'] = df['coc_code'].str[:2]

    PEL = pd.read_excel(PEL_path, sheet_name='Sheet1')

    df_merged = df.merge(PEL, on=['state'], how='left')

    return df_merged



def main():
    paths = get_project_paths()
    df = load_HUD_data(paths["raw_data"] / "HUD.xlsx")
    df = rename_HUD_columns(df, paths["raw_data"] / "policy_scores_PEL.xlsx")
    df.to_stata(paths['clean_data'] / "HUD_clean.dta")

if __name__ == "__main__":
    main()