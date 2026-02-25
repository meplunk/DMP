import os
import requests
import pandas as pd
from pathlib import Path
from config import STATE_CLEAN, UNEMP, UNEMP, STATE_POP_10, STATE_POP_20, COVID
import time
from tqdm import tqdm
import numpy as np

state_fips_dict = {
    "Alabama": "01",
    "Alaska": "02",
    "Arizona": "04",
    "Arkansas": "05",
    "California": "06",
    "Colorado": "08",
    "Connecticut": "09",
    "Delaware": "10",
    "District of Columbia": "11",
    "Florida": "12",
    "Georgia": "13",
    "Hawaii": "15",
    "Idaho": "16",
    "Illinois": "17",
    "Indiana": "18",
    "Iowa": "19",
    "Kansas": "20",
    "Kentucky": "21",
    "Louisiana": "22",
    "Maine": "23",
    "Maryland": "24",
    "Massachusetts": "25",
    "Michigan": "26",
    "Minnesota": "27",
    "Mississippi": "28",
    "Missouri": "29",
    "Montana": "30",
    "Nebraska": "31",
    "Nevada": "32",
    "New Hampshire": "33",
    "New Jersey": "34",
    "New Mexico": "35",
    "New York": "36",
    "North Carolina": "37",
    "North Dakota": "38",
    "Ohio": "39",
    "Oklahoma": "40",
    "Oregon": "41",
    "Pennsylvania": "42",
    "Rhode Island": "44",
    "South Carolina": "45",
    "South Dakota": "46",
    "Tennessee": "47",
    "Texas": "48",
    "Utah": "49",
    "Vermont": "50",
    "Virginia": "51",
    "Washington": "53",
    "West Virginia": "54",
    "Wisconsin": "55",
    "Wyoming": "56",
    "Puerto Rico": "72",
    "Virgin Islands": "78",
    "U.S. Virgin Islands": "78"
}


def population_data(): 
    pop10 = pd.read_excel(STATE_POP_10, sheet_name="NST-EST2020INT-POP", skiprows=3)
    pop20 = pd.read_excel(STATE_POP_20, sheet_name="NST-EST2024-POP",skiprows=3)

    pop10 = pop10.rename(columns={'Unnamed: 0': 'state', 2016: 'POP_2016', 2017: 'POP_2017', 2018: 'POP_2018', 2019: 'POP_2019'})
    pop20 = pop20.rename(columns={'Unnamed: 0': 'state', 2020: 'POP_2020', 2021: 'POP_2021', 2022: 'POP_2022', 2023: 'POP_2023'})

    pop10['state'] = pop10['state'].str.strip('.')
    pop20['state'] = pop20['state'].str.strip('.')

    pop10['state_fips'] = pop10['state'].map(state_fips_dict)
    pop20['state_fips'] = pop20['state'].map(state_fips_dict)
    
    keepcols10 = ['state', 'state_fips',  'POP_2016', 'POP_2017', 'POP_2018', 'POP_2019']
    keepcols20 = ['state_fips', 'POP_2020', 'POP_2021', 'POP_2022', 'POP_2023']

    pop10 = pop10[keepcols10]
    pop20 = pop20[keepcols20]

    # drop if not a state (state_fips == NaN)
    pop10 = pop10.dropna(subset=['state_fips'])
    pop20 = pop20.dropna(subset=['state_fips'])

    # change POP columns float --> int
    for col in ['POP_2016', 'POP_2017', 'POP_2018', 'POP_2019']:
        pop10[col] = pop10[col].astype(int)
    for col in ['POP_2020', 'POP_2021', 'POP_2022', 'POP_2023']:
        pop20[col] = pop20[col].astype(int)

    # merge pop10 and pop20 on state_fips
    df = pd.merge(pop10, pop20, on='state_fips', how='left')

    return df


def unemployment_data(df):
    def clean_unemployment_data(path, sheetname, year):
        df = pd.read_excel(path, sheet_name=sheetname, skiprows=1)
        # Example cleaning steps (these would depend on the actual data format)
        df = df.rename(columns={"State FIPS Code": "state_fips", "County FIPS Code": "countyfips", 
                                "Unemployed": f"UNEMP_{year}", "Labor Force": f"LF_{year}"})
        df = df[["state_fips", f"UNEMP_{year}", f"LF_{year}"]]

        # group by state (sum UNEMP and LF for all obs with the same fips code)
        df = df.groupby("state_fips").sum().reset_index()
        
        # change LF and UNEMP to float64, coerce
        # Coerce to numeric (non-numeric becomes NaN)
        df[f'UNEMP_{year}'] = pd.to_numeric(df[f'UNEMP_{year}'], errors='coerce')
        df[f'LF_{year}'] = pd.to_numeric(df[f'LF_{year}'], errors='coerce')

        # Drop rows where either is missing
        df = df.dropna(subset=[f'UNEMP_{year}', f'LF_{year}'])

        # Now compute unemployment rate
        df[f'U3_{year}'] = df[f'UNEMP_{year}'] / df[f'LF_{year}'] * 100
        df[f'U3_{year}'] = df[f'U3_{year}'].round(2)

        # change state fips to string and pad with zeros (2), for example 1 --> '01'
        df['state_fips'] = (
            df['state_fips']
                .astype(int)
                .astype(str)
                .str.zfill(2)
        )

        # drop LF and UNEMP columns
        df = df.drop(columns=[f'UNEMP_{year}', f'LF_{year}'])

        return df
        
    years = range(2016, 2024)

    for year in years:
        file_suffix = str(year)[-2:]  # 2016 → "16"
        
        unemp = clean_unemployment_data(
            UNEMP / f"laucnty{file_suffix}.xlsx",
            f"laucnty{file_suffix}",
            year
        )
        
        df = df.merge(unemp, on="state_fips", how="left")
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

    # drop the last three digits of GEOID and rename the column 'state'
    df['state_fips'] = df['GEOID'].str[:-3]
    df = df.drop(columns=["GEOID"])

    # group by state_fips, sum
    df = df.groupby("state_fips").sum().reset_index()

    # make the COVID_deaths_{year} column float --> int
    df[f'COVID_deaths_{year}'] = df[f'COVID_deaths_{year}'].astype(int)

    return df


def add_covid_data(df):
    """
    Add annual county-level COVID cases/deaths to df using GEOID merge.
    Produces COVID_cases_YYYY and COVID_deaths_YYYY for YYYY=2020..2023 plus zeros for 2016..2019.
    """
    # Merge annual totals (2020-2023)
    for year in [2020, 2021, 2022, 2023]:
        covid_year = clean_covid_data(COVID / f"us-counties-{year}.csv", year)
        df = df.merge(covid_year, on="state_fips", how="left")

    # Add pre-COVID years explicitly as zeros
    for year in [2016, 2017, 2018, 2019]:
        df[f"COVID_cases_{year}"] = 0
        df[f"COVID_deaths_{year}"] = 0

    return df

def main():
    df = population_data()
    df = unemployment_data(df)
    df = add_covid_data(df)
    # reshape df wide to long, create a year column from the suffixes
    df_long = pd.wide_to_long(
        df,
        stubnames=["POP", "U3", "COVID_cases", "COVID_deaths"],
        i="state_fips",
        j="year",
        sep="_",
        suffix="\\d+"
    ).reset_index()

    df_long["year"] = df_long["year"].astype(int)
    print(df_long.head())
    df_long.to_csv(STATE_CLEAN, index=False)
    print(f'saved to {STATE_CLEAN}')


if __name__ == "__main__":
    main()  