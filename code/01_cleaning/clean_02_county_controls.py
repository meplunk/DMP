import os
import requests
import pandas as pd
from pathlib import Path
from config import COUNTY_CLEAN, UNEMP, UNEMP, COUNTY_POP_2020s, COUNTY_POP_2010s, COVID, CROSSWALK, COVARIATES
import time
from tqdm import tqdm
import numpy as np

def add_population_data():
    pop20 = pd.read_csv(COUNTY_POP_2020s, encoding="latin-1")
    pop10 = pd.read_csv(COUNTY_POP_2010s, encoding="latin-1")

    keepcols10 = ["STATE", "COUNTY", "STNAME", "CTYNAME", "POPESTIMATE2016", "POPESTIMATE2017", "POPESTIMATE2018", 
                  "POPESTIMATE2019"]
    keepcols20 = ["STATE", "COUNTY", "POPESTIMATE2020", "POPESTIMATE2021", "POPESTIMATE2022", "POPESTIMATE2023"]

    pop20 = pop20[keepcols20]
    pop10 = pop10[keepcols10]

    pop20["STATE"] = pop20["STATE"].astype(str).str.zfill(2)
    pop20["COUNTY"] = pop20["COUNTY"].astype(str).str.zfill(3)
    pop10["STATE"] = pop10["STATE"].astype(str).str.zfill(2)
    pop10["COUNTY"] = pop10["COUNTY"].astype(str).str.zfill(3)

    pop20 = pop20.rename(columns={'STATE': 'statefips', 'COUNTY': 'countyfips', 'POPESTIMATE2020': 'POP_2020', 
                                  'POPESTIMATE2021': 'POP_2021', 'POPESTIMATE2022': 'POP_2022', 'POPESTIMATE2023': 'POP_2023'})
    pop10 = pop10.rename(columns={'STATE': 'statefips', 'COUNTY': 'countyfips', 'POPESTIMATE2016': 'POP_2016', 
                                  'POPESTIMATE2017': 'POP_2017', 'POPESTIMATE2018': 'POP_2018', 'POPESTIMATE2019': 'POP_2019'})

    df = pd.read_csv(CROSSWALK)
    df_ = df.copy()
    df["statefips"] = df["statefips"].astype(str).str.zfill(2)
    df["countyfips"] = df["countyfips"].astype(str).str.zfill(3)
    df = pd.merge(df, pop10, on=['statefips', 'countyfips'], how='left')
    df = pd.merge(df, pop20, on=['statefips', 'countyfips'], how='left')

    n_unmatched = df["POP_2021"].isna().sum()
    print("Counties rows with no match in population:", n_unmatched)

    return df

def clean_unemployment_data(path, sheetname, year):
    df = pd.read_excel(path, sheet_name=sheetname, skiprows=1)
    # Example cleaning steps (these would depend on the actual data format)
    df = df.rename(columns={"State FIPS Code": "statefips", "County FIPS Code": "countyfips", 
                            "Unemployment Rate (%)": f"UNEMP_{year}"})
    df["statefips"] = df["statefips"].fillna(0).astype(int)
    df["countyfips"] = df["countyfips"].fillna(0).astype(int)
    df["statefips"] = df["statefips"].astype(str).str.zfill(2)
    df["countyfips"] = df["countyfips"].astype(str).str.zfill(3)
    return df[["statefips", "countyfips", f"UNEMP_{year}"]]


def add_unemployment_data(df):
    unemp_2016 = clean_unemployment_data(UNEMP / "laucnty16.xlsx", "laucnty16", 2016)
    unemp_2017 = clean_unemployment_data(UNEMP / "laucnty17.xlsx", "laucnty17", 2017)
    unemp_2018 = clean_unemployment_data(UNEMP / "laucnty18.xlsx", "laucnty18", 2018)
    unemp_2019 = clean_unemployment_data(UNEMP / "laucnty19.xlsx", "laucnty19", 2019)
    unemp_2020 = clean_unemployment_data(UNEMP / "laucnty20.xlsx", "laucnty20", 2020)
    unemp_2021 = clean_unemployment_data(UNEMP / "laucnty21.xlsx", "laucnty21", 2021)
    unemp_2022 = clean_unemployment_data(UNEMP / "laucnty22.xlsx", "laucnty22", 2022)
    unemp_2023 = clean_unemployment_data(UNEMP / "laucnty23.xlsx", "laucnty23", 2023)
    df["statefips"] = df["statefips"].astype(str).str.zfill(2)
    df["countyfips"] = df["countyfips"].astype(str).str.zfill(3)
    df = pd.merge(df, unemp_2016, on=['statefips', 'countyfips'], how='left')
    df = pd.merge(df, unemp_2017, on=['statefips', 'countyfips'], how='left')
    df = pd.merge(df, unemp_2018, on=['statefips', 'countyfips'], how='left')
    df = pd.merge(df, unemp_2019, on=['statefips', 'countyfips'], how='left')   
    df = pd.merge(df, unemp_2020, on=['statefips', 'countyfips'], how='left')
    df = pd.merge(df, unemp_2021, on=['statefips', 'countyfips'], how='left')
    df = pd.merge(df, unemp_2022, on=['statefips', 'countyfips'], how='left')
    df = pd.merge(df, unemp_2023, on=['statefips', 'countyfips'], how='left')
    return df

def clean_covid_data(path, year):
    """
    Read NYT us-counties-YYYY.csv and return annual totals by county FIPS (GEOID).

    Assumption (NYT yearly files): 'cases' and 'deaths' are cumulative within the year.
    Therefore, max(cases) in that file = total cases during that year.
    """
    df = pd.read_csv(path, dtype={"fips": "Int64"})

    # Keep only what we need
    df = df.rename(columns={"fips": "GEOID"})
    df = df[["GEOID", "cases", "deaths"]].copy()

    # Drop rows without a county FIPS (NYT has some NaNs for "Unknown" etc.)
    df = df.dropna(subset=["GEOID"])

    # Standardize GEOID as 5-char string
    df["GEOID"] = df["GEOID"].astype(int).astype(str).str.zfill(5)

    # End-of-year totals (within-year cumulative)
    df = df.groupby("GEOID", as_index=False)[["cases", "deaths"]].max()

    # Rename to annual totals columns
    df = df.rename(columns={
        "cases": f"COVID_cases_{year}",
        "deaths": f"COVID_deaths_{year}"
    })

    return df


def add_covid_data(df, covid_dir):
    """
    Add annual county-level COVID cases/deaths to df using GEOID merge.
    Produces COVID_cases_YYYY and COVID_deaths_YYYY for YYYY=2020..2023 plus zeros for 2016..2019.
    """
    # Build county GEOID in your main df
    df = df.copy()
    df["GEOID"] = (
        df["statefips"].astype(str).str.zfill(2)
        + df["countyfips"].astype(str).str.zfill(3)
    )

    # Merge annual totals (2020-2023)
    for year in [2020, 2021, 2022, 2023]:
        covid_year = clean_covid_data(covid_dir / f"us-counties-{year}.csv", year)
        df = df.merge(covid_year, on="GEOID", how="left")

        # Counties missing from NYT file -> treat as 0
        df[f"COVID_cases_{year}"] = df[f"COVID_cases_{year}"].fillna(0)
        df[f"COVID_deaths_{year}"] = df[f"COVID_deaths_{year}"].fillna(0)

    # Add pre-COVID years explicitly as zeros
    for year in [2016, 2017, 2018, 2019]:
        df[f"COVID_cases_{year}"] = 0
        df[f"COVID_deaths_{year}"] = 0

    # Optionally drop GEOID if you don't need it downstream
    df = df.drop(columns=["GEOID"])

    return df

def collapsing_by_coc(df):

    # ------------------------------------------------------------
    # Drop unused geographic columns
    # ------------------------------------------------------------
    cols_to_drop = ['statefips', 'countyfips', 'STNAME', 'CTYNAME']
    df = df.drop(columns=[c for c in cols_to_drop if c in df.columns])

    # ------------------------------------------------------------
    # Clean numeric types
    # ------------------------------------------------------------
    for col in df.columns:
        if col.startswith(('POP_', 'COVID_')):
            df[col] = pd.to_numeric(df[col], errors='coerce')
        elif col.startswith('UNEMP_'):
            df[col] = pd.to_numeric(
                df[col].replace('N.A.', np.nan),
                errors='coerce'
            )

    # ------------------------------------------------------------
    # Identify year suffixes dynamically
    # ------------------------------------------------------------
    pop_cols = sorted([c for c in df.columns if c.startswith('POP_')])
    years = sorted([c.split('_')[1] for c in pop_cols])

    # ------------------------------------------------------------
    # Aggregate POP and COVID by sum
    # ------------------------------------------------------------
    sum_cols = [c for c in df.columns if c.startswith(('POP_', 'COVID_'))]
    df_sum = df.groupby("coc_id", as_index=False)[sum_cols].sum()

    # ------------------------------------------------------------
    # Population-weighted unemployment
    # ------------------------------------------------------------
    for year in years:
        pop_col = f'POP_{year}'
        unemp_col = f'UNEMP_{year}'

        # Create weighted numerator at county level
        df[f'_weighted_unemp_{year}'] = (
            (df[unemp_col] / 100) * df[pop_col]
        )

        # Sum numerator by CoC
        num = (
            df.groupby("coc_id")[f'_weighted_unemp_{year}']
              .sum()
              .reset_index(name=f'_num_{year}')
        )

        df_sum = df_sum.merge(num, on="coc_id", how="left")

        # Compute weighted unemployment
        df_sum[f'UNEMP_{year}'] = (
            df_sum[f'_num_{year}'] / df_sum[pop_col]
        ) * 100

        df_sum = df_sum.drop(columns=[f'_num_{year}'])
    
    print("Columns after aggregation:")
    print(df_sum.columns)

    # ------------------------------------------------------------
    # NOW RESHAPE TO LONG
    # ------------------------------------------------------------
    value_cols = (
        [c for c in df_sum.columns if c.startswith("POP_")] +
        [c for c in df_sum.columns if c.startswith("COVID_")] +
        [c for c in df_sum.columns if c.startswith("UNEMP_")]
    )

    df_long = df_sum.melt(
        id_vars="coc_id",
        value_vars=value_cols,
        var_name="variable",
        value_name="value"
    )

    df_long[["var", "year"]] = df_long["variable"].str.rsplit("_", n=1, expand=True)
    df_long["year"] = df_long["year"].astype(int)

    df_long = (
        df_long
        .pivot(index=["coc_id","year"], columns="var", values="value")
        .reset_index()
    )

    # Now this WILL work
    covid_cols = [c for c in df_long.columns if c.startswith("COVID")]
    df_long.loc[df_long["year"] < 2020, covid_cols] = 0

    return df_long


# -----------------------------
# MAIN
# -----------------------------

def main():
    df = add_population_data()
    df = add_unemployment_data(df)
    df = add_covid_data(df)
    df.to_csv(COUNTY_CLEAN, index=False)
    print(df.head())
    df_coc = collapsing_by_coc(df)
    df_coc.to_csv(COVARIATES, index=False)
    print(df_coc.head())

if __name__ == "__main__":
    main()