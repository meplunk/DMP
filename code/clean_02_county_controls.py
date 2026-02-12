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
    df = pd.read_csv(path)
    df = df.rename(columns={"fips": "GEOID", "cases": f"COVID_cases_{year}", "deaths": f"COVID_deaths_{year}"})
    df = df[["GEOID", f"COVID_cases_{year}", f"COVID_deaths_{year}"]]
    df["GEOID"] = df["GEOID"].fillna(0).astype(int)
    df["GEOID"] = df["GEOID"].astype(str).str.zfill(5)  
    df = df.groupby("GEOID", as_index=False).sum()
    return df

def add_covid_data(df):
    covid_2020 = clean_covid_data(COVID / "us-counties-2020.csv", 2020)
    covid_2021 = clean_covid_data(COVID / "us-counties-2021.csv", 2021)
    covid_2022 = clean_covid_data(COVID / "us-counties-2022.csv", 2022)
    covid_2023 = clean_covid_data(COVID / "us-counties-2023.csv", 2023)

    df["GEOID"] = df["statefips"].astype(str).str.zfill(2) + df["countyfips"].astype(str).str.zfill(3)

    df['COVID_cases_2016'] = 0
    df['COVID_deaths_2016'] = 0
    df['COVID_cases_2017'] = 0
    df['COVID_deaths_2017'] = 0
    df['COVID_cases_2018'] = 0
    df['COVID_deaths_2018'] = 0
    df['COVID_cases_2019'] = 0
    df['COVID_deaths_2019'] = 0
    df["statefips"] = df["statefips"].astype(str).str.zfill(2)
    df["countyfips"] = df["countyfips"].astype(str).str.zfill(3)
    df = pd.merge(df, covid_2020, on="GEOID", how='left')
    df = pd.merge(df, covid_2021, on="GEOID", how='left')
    df = pd.merge(df, covid_2022, on="GEOID", how='left')
    df = pd.merge(df, covid_2023, on="GEOID", how='left')

    df = df.drop(columns=["GEOID"])

    return df

def collapsing_by_coc(df):
    import numpy as np
    import pandas as pd

    # ------------------------------------------------------------
    # Drop unused geographic columns
    # ------------------------------------------------------------
    cols_to_drop = ['statefips', 'countyfips', 'STNAME', 'CTYNAME']
    df = df.drop(columns=cols_to_drop)

    # ------------------------------------------------------------
    # Clean types
    # ------------------------------------------------------------
    for col in df.columns:
        if col.startswith('POP') or col.startswith('COVID'):
            df[col] = pd.to_numeric(df[col], errors='coerce')
        elif col.startswith('UNEMP'):
            df[col] = pd.to_numeric(
                df[col].replace('N.A.', np.nan),
                errors='coerce'
            )

    # ------------------------------------------------------------
    # Identify years dynamically
    # ------------------------------------------------------------
    pop_cols = [c for c in df.columns if c.startswith('POP_')]
    unemp_cols = [c for c in df.columns if c.startswith('UNEMP_')]
    covid_cols = [c for c in df.columns if c.startswith('COVID_')]

    # Extract year suffixes
    years = [c.split('_')[1] for c in pop_cols]

    # ------------------------------------------------------------
    # First aggregate POP and COVID normally (sum)
    # ------------------------------------------------------------
    agg_dict = {col: 'sum' for col in pop_cols + covid_cols}
    df_sum = df.groupby("coc_id", as_index=False).agg(agg_dict)

    # ------------------------------------------------------------
    # Now compute population-weighted unemployment
    # ------------------------------------------------------------
    weighted_unemp = []

    for year in years:
        pop_col = f'POP_{year}'
        unemp_col = f'UNEMP_{year}'

        # Compute numerator: sum( (UNEMP/100) * POP )
        df[f'_unemp_weighted_{year}'] = (
            (df[unemp_col] / 100) * df[pop_col]
        )

        num = (
            df.groupby("coc_id")[f'_unemp_weighted_{year}']
              .sum()
              .reset_index(name=f'_num_{year}')
        )

        # Merge numerator
        df_sum = df_sum.merge(num, on="coc_id", how="left")

        # Final weighted unemployment rate
        df_sum[f'UNEMP_{year}'] = (
            df_sum[f'_num_{year}'] / df_sum[pop_col]
        ) * 100

        # Drop temporary numerator column
        df_sum = df_sum.drop(columns=[f'_num_{year}'])

    return df_sum


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