from pathlib import Path

# ============================================
# BASE PATHS
# ============================================
PROJECT_ROOT = Path(__file__).parent.parent
DATA_DIR = PROJECT_ROOT / "data"
CODE_DIR = PROJECT_ROOT / "code"

# ============================================
# DATA PATHS
# ============================================
RAW_DIR = DATA_DIR / "01_raw"
CLEAN_DIR = DATA_DIR / "02_cleaned"

HUD_DATA = RAW_DIR / "HUD.xlsx"
COC_SHP = RAW_DIR / "CoC_GIS_National_Boundary.gdb"
COUNTY_SHP = RAW_DIR / "tl_2023_us_county/tl_2023_us_county.shp"
COUNTY_POP_2020s = RAW_DIR / "population" / "co-est2024-alldata.csv"
COUNTY_POP_2010s = RAW_DIR / "population" / "co-est2019-alldata.csv"
UNEMP = RAW_DIR / "unemployment"
COVID = RAW_DIR / "covid"

HUD_CLEAN = CLEAN_DIR / "HUD_clean.csv"
CROSSWALK = CLEAN_DIR / "coc_county_crosswalk.csv"
COUNTY_CLEAN = CLEAN_DIR / "county_level_data.csv"
COVARIATES = CLEAN_DIR / "covariates.csv"