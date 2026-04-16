from pathlib import Path

# ============================================
# BASE PATHS
# ============================================
PROJECT_ROOT = Path(__file__).parent.parent.parent
DATA_DIR = PROJECT_ROOT / "data"
CODE_DIR = PROJECT_ROOT / "code"

# ============================================
# DATA PATHS
# ============================================
RAW_DIR = DATA_DIR / "01_raw"
CLEAN_DIR = DATA_DIR / "02_cleaned"

# ============================================
# RAW DATA
# ============================================
HUD_DATA = RAW_DIR / "HUD.xlsx"
COC_SHP = RAW_DIR / "CoC_GIS_National_Boundary.gdb"
COUNTY_SHP = RAW_DIR / "tl_2023_us_county/tl_2023_us_county.shp"
COUNTY_POP_2020s = RAW_DIR / "population" / "co-est2024-alldata.csv"
COUNTY_POP_2010s = RAW_DIR / "population" / "co-est2019-alldata.csv"
UNEMP = RAW_DIR / "unemployment"
COVID = RAW_DIR / "covid"
STATE_POLICY = RAW_DIR / "2022.03.01 Moratoria + Supportive Measures Datasets.xlsx"

# ============================================
# CLEAN DATA
# ============================================
HUD_CLEAN = CLEAN_DIR / "HUD_only.csv"
CROSSWALK = CLEAN_DIR / "coc_county_crosswalk.csv"
COUNTY_CLEAN = CLEAN_DIR / "county_level_data.csv"
COVARIATES = CLEAN_DIR / "covariates.csv"
POLICY_PANEL = CLEAN_DIR / "policy_panel.csv"
ALL_DATA = CLEAN_DIR / "all_data.dta"
ALL_STATE_DATA = CLEAN_DIR / "all_state_data.dta"

# ============================================
# OUTPUT
# ============================================
TABLES = PROJECT_ROOT / "output" / "tables" / "tex"
GRAPHS = PROJECT_ROOT / "output" / "graphs"


# ============================================
# VARIABLE LISTS FOR REGRESSIONS
# ============================================
OUTCOMES = [
    "inflow_rate",
    "median_days_homeless",
    "exit_rate",
    "perm_exit_rate"
]

POLICY_VARS = [
    "overall_days", "s1_days", "s1_CVD_days", "s1_NP_days",
    "s2_days", "s2_CVD_days", "s2_NP_days",
    "s3_days", "s3_CVD_days", "s3_NP_days",
    "s4_days", "s5_days", "cares_days", "cdc_days",
    "moratorium_intensity", "weighted_scorecard",
    "share_overall_days", "share_s1_days", "share_s1_CVD_days", "share_s1_NP_days",
    "share_s2_days", "share_s2_CVD_days", "share_s2_NP_days",
    "share_s3_days", "share_s3_CVD_days", "share_s3_NP_days",
    "share_s4_days", "share_s5_days", "share_cares_days", "share_cdc_days",
    "share_moratorium_intensity"
]


TO_SHARE = [
    "overall_days", "s1_days", "s1_CVD_days", "s1_NP_days",
    "s2_days", "s2_CVD_days", "s2_NP_days",
    "s3_days", "s3_CVD_days", "s3_NP_days",
    "s4_days", "s5_days", "cares_days", "cdc_days"
]

CONTROLS = ["U3", "COVID_cases"]

VAR_LABELS = {
    "inflow_rate": "Inflow",
    "median_days_homeless": r"\shortstack{\\[0.1ex] Median \\ Days Homeless}",
    "avg_days_homeless": r"\shortstack{\\[0.1ex] Average \\ Days Homeless}",
    "exit_rate": "Exits",
    "perm_exit_rate": r"\shortstack{\\[0.1ex] Exits to \\ Permanent Housing}",
    "U3": "Unemployment Rate",
    "COVID_cases": "COVID Cases",
    "moratorium_intensity": "Moratorium Intensity",
    "share_moratorium_intensity": "Moratorium Intensity by Share of Year",
    "SCORECARD": "Scorecard",
    "weighted_scorecard": "Weighted Scorecard",
    "overall_days": "Overall Moratorium Days",
    "s1_days": "Stage 1 Days",
    "s1_CVD_days": "Stage 1 COVID Days",
    "s1_NP_days": "Stage 1 Non-Payment Days",
    "s2_days": "Stage 2 Days",
    "s2_CVD_days": "Stage 2 COVID Days",
    "s2_NP_days": "Stage 2 Non-Payment Days",
    "s3_days": "Stage 3 Days",
    "s3_CVD_days": "Stage 3 COVID Days",
    "s3_NP_days": "Stage 3 Non-Payment Days",
    "s4_days": "Stage 4 Days",
    "s5_days": "Stage 5 Days",
    "cares_days": "CARES Act Days",
    "cdc_days": "CDC Moratorium Days",
    "share_overall_days": "Share of Year with Any Moratorium",
    "share_s1_days": "Share of Year with Stage 1 Moratorium",
    "share_s1_CVD_days": "Share of Year with Stage 1 COVID Moratorium",
    "share_s1_NP_days": "Share of Year with Stage 1 Non-Payment Moratorium",
    "share_s2_days": "Share of Year with Stage 2 Moratorium",
    "share_s2_CVD_days": "Share of Year Stage 2 COVID Moratorium",
    "share_s2_NP_days": "Share of Year with Stage 2 Non-Payment Moratorium",
    "share_s3_days": "Share of Year with Stage 3 Moratorium",
    "share_s3_CVD_days": "Share of Year with Stage 3 COVID Moratorium",
    "share_s3_NP_days": "Share of Year with Stage 3 Non-Payment Moratorium",
    "share_s4_days": "Share of Year with Stage 4 Moratorium",
    "share_s5_days": "Share of Year with Stage 5 Moratorium",
    "share_cares_days": "Share of Year with CARES Act Moratorium",
    "share_cdc_days": "Share of Year with CDC Moratorium",
}