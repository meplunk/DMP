import os
import requests
import pandas as pd
from pathlib import Path
from config import COUNTY_CLEAN, UNEMP, UNEMP, STATE_POP_10, STATE_POP_20, COVID, CROSSWALK, COVARIATES
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
    
    unemp_2016 = clean_unemployment_data(UNEMP / "laucnty16.xlsx", "laucnty16", 2016)
    unemp_2017 = clean_unemployment_data(UNEMP / "laucnty17.xlsx", "laucnty17", 2017)
    unemp_2018 = clean_unemployment_data(UNEMP / "laucnty18.xlsx", "laucnty18", 2018)
    unemp_2019 = clean_unemployment_data(UNEMP / "laucnty19.xlsx", "laucnty19", 2019)
    unemp_2020 = clean_unemployment_data(UNEMP / "laucnty20.xlsx", "laucnty20", 2020)
    unemp_2021 = clean_unemployment_data(UNEMP / "laucnty21.xlsx", "laucnty21", 2021)
    unemp_2022 = clean_unemployment_data(UNEMP / "laucnty22.xlsx", "laucnty22", 2022)
    unemp_2023 = clean_unemployment_data(UNEMP / "laucnty23.xlsx", "laucnty23", 2023)
    df = pd.merge(df, unemp_2016, on=['state_fips'], how='left')
    df = pd.merge(df, unemp_2017, on=['state_fips'], how='left')
    df = pd.merge(df, unemp_2018, on=['state_fips'], how='left')
    df = pd.merge(df, unemp_2019, on=['state_fips'], how='left')
    df = pd.merge(df, unemp_2020, on=['state_fips'], how='left')
    df = pd.merge(df, unemp_2021, on=['state_fips'], how='left')
    df = pd.merge(df, unemp_2022, on=['state_fips'], how='left')
    df = pd.merge(df, unemp_2023, on=['state_fips'], how='left')
    return df


def main():
    df = population_data()
    df = unemployment_data(df)
    print(df.head())



if __name__ == "__main__":
    main()  