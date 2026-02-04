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

HUD_CLEAN = CLEAN_DIR / "HUD_clean.csv"
COUNTY_CLEAN = CLEAN_DIR / "county_level_data.csv"