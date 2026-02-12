"""
Title: clean_03_policy_panel.py
Author: Mary Edith Plunkett
Updated: 2026-02
"""

from __future__ import annotations

import re
from typing import Dict, Tuple, Iterable
import pandas as pd
import numpy as np

from config import STATE_POLICY, POLICY_PANEL

# ---------------------------------------------------------------------
# SETTINGS
# ---------------------------------------------------------------------

START = pd.Timestamp("2020-01-01")
END   = pd.Timestamp("2022-12-31")
SHEET_NAME = "Moratoria Dataset"

# 50 states + DC + PR + VI
STATE_CODE_MAP = {
    "Alabama": "AL", "Alaska": "AK", "Arizona": "AZ", "Arkansas": "AR",
    "California": "CA", "Colorado": "CO", "Connecticut": "CT",
    "Delaware": "DE", "District of Columbia": "DC",
    "Florida": "FL", "Georgia": "GA", "Hawaii": "HI",
    "Idaho": "ID", "Illinois": "IL", "Indiana": "IN",
    "Iowa": "IA", "Kansas": "KS", "Kentucky": "KY",
    "Louisiana": "LA", "Maine": "ME", "Maryland": "MD",
    "Massachusetts": "MA", "Michigan": "MI", "Minnesota": "MN",
    "Mississippi": "MS", "Missouri": "MO", "Montana": "MT",
    "Nebraska": "NE", "Nevada": "NV", "New Hampshire": "NH",
    "New Jersey": "NJ", "New Mexico": "NM", "New York": "NY",
    "North Carolina": "NC", "North Dakota": "ND", "Ohio": "OH",
    "Oklahoma": "OK", "Oregon": "OR", "Pennsylvania": "PA",
    "Rhode Island": "RI", "South Carolina": "SC",
    "South Dakota": "SD", "Tennessee": "TN",
    "Texas": "TX", "Utah": "UT", "Vermont": "VT",
    "Virginia": "VA", "Washington": "WA", "West Virginia": "WV",
    "Wisconsin": "WI", "Wyoming": "WY",
    "Puerto Rico": "PR",
    "Virgin Islands": "VI",
    "U.S. Virgin Islands": "VI"
}

STATE_POLICY_COLUMNS: Dict[str, Tuple[str, str]] = {
    "overall_active": ("Overall First Date of Effect", "Overall Date of Expiration"),
    "s1_active": ("S1 First Date of Effect", "S1 Date of Expiration"),
    "s1_CVD_active": ("S1 COVID-19 Hardship First Date of Effect", "S1 COVID-19 Hardship Date of Expiration"),
    "s1_NP_active": ("S1 Non-Payment Only First Date of Effect", "S1 Non-Payment Only Date of Expiration"),
    "s2_active": ("S2 First Date of Effect", "S2 Date of Expiration"),
    "s2_CVD_active": ("S2 COVID-19 Hardship First Date of Effect", "S2 COVID-19 Hardship Date of Expiration"),
    "s2_NP_active": ("S2 Non-Payment Only First Date of Effect", "S2 Non-Payment Only Date of Expiration"),
    "s3_active": ("S3 First Date of Effect", "S3 Date of Expiration"),
    "s3_CVD_active": ("S3 COVID-19 Hardship First Date of Effect", "S3 COVID-19 Hardship Date of Expiration"),
    "s3_NP_active": ("S3 Non-Payment Only First Date of Effect", "S3 Non-Payment Only Date of Expiration"),
    "s4_active": ("S4 First Date of Effect", "S4 Date of Expiration"),
    "s5_active": ("S5 First Date of Effect", "S5 Date of Expiration"),
    "cares_active": ("CARES Act Pleading Req't First Date of Effect", "CARES Act Pleading Req't Expiration"),
    "cdc_active": ("CDC Moratorium Recognition First Date of Effect", "CDC Moratorium Recognition Date of Expiration"),
}


# ---------------------------------------------------------------------
# HELPERS
# ---------------------------------------------------------------------

def clean_state_name(s: object) -> str:
    """Turn 'California 3' -> 'California'."""
    if pd.isna(s):
        return ""
    s = str(s).strip()
    s = re.sub(r"\s+\d+$", "", s)
    return s.strip()


def parse_mixed_us_date(x: object) -> pd.Timestamp:
    """Safely parse MM/DD/YYYY and MM/DD/YY formats."""
    if pd.isna(x):
        return pd.NaT

    if isinstance(x, pd.Timestamp):
        return x

    s = str(x).strip()
    if not s:
        return pd.NaT

    for fmt in ("%m/%d/%Y", "%m/%d/%y"):
        try:
            return pd.to_datetime(s, format=fmt)
        except Exception:
            continue

    return pd.to_datetime(s, errors="coerce")


def flag_policy_window(
    state_daily: pd.DataFrame,
    policy_df: pd.DataFrame,
    start_col: str,
    end_col: str,
    out_col: str,
) -> None:
    """Assign 1 for days covered by policy window (overlap safe)."""

    for _, row in policy_df.iterrows():
        start = row[start_col]
        end = row[end_col]

        if pd.isna(start):
            continue
        if pd.isna(end):
            end = END

        start = max(start, START)
        end = min(end, END)

        if end < start:
            continue

        mask = (
            (state_daily["state_code"] == row["state_code"]) &
            (state_daily["date"] >= start) &
            (state_daily["date"] <= end)
        )
        state_daily.loc[mask, out_col] = 1


# ---------------------------------------------------------------------
# MAIN BUILDER
# ---------------------------------------------------------------------

def clean_state_policy() -> pd.DataFrame:

    df = pd.read_excel(STATE_POLICY, sheet_name=SHEET_NAME)

    # Clean state names
    df["state_clean"] = df["State"].apply(clean_state_name)
    df["state_code"] = df["state_clean"].map(STATE_CODE_MAP)

    # Keep only mapped states/territories
    df = df[df["state_code"].notna()].copy()

    # Parse all date columns
    date_cols = sorted({c for pair in STATE_POLICY_COLUMNS.values() for c in pair})
    for col in date_cols:
        df[col] = df[col].apply(parse_mixed_us_date)

    # Build daily skeleton
    all_days = pd.date_range(START, END, freq="D")

    state_daily = (
        df[["state_code"]]
        .drop_duplicates()
        .assign(key=1)
        .merge(pd.DataFrame({"date": all_days, "key": 1}), on="key")
        .drop(columns="key")
    )

    # Flag policies
    for out_col, (start_col, end_col) in STATE_POLICY_COLUMNS.items():
        state_daily[out_col] = 0
        flag_policy_window(
            state_daily=state_daily,
            policy_df=df[["state_code", start_col, end_col]],
            start_col=start_col,
            end_col=end_col,
            out_col=out_col,
        )

    # Enforce binary
    for col in STATE_POLICY_COLUMNS.keys():
        state_daily[col] = state_daily[col].clip(0, 1).astype(int)

    # Aggregate annually
    state_daily["year"] = state_daily["date"].dt.year

    agg_spec = {
        f"{k.replace('_active','')}_days": (k, "sum")
        for k in STATE_POLICY_COLUMNS.keys()
    }

    agg_spec["total_days"] = ("date", "count")

    annual = (
        state_daily
        .groupby(["state_code", "year"], as_index=False)
        .agg(**agg_spec)
        .sort_values(["state_code", "year"])
    )

    annual.to_csv(POLICY_PANEL, index=False)

    return annual


def main() -> None:
    out = clean_state_policy()
    print(f"✓ Wrote policy panel to {POLICY_PANEL}")
    print(f"States/Territories: {out['state_code'].nunique()} | Years: {out['year'].nunique()}")


if __name__ == "__main__":
    main()
