from pathlib import Path

# ============================================
# BASE PATHS
# ============================================
PROJECT_ROOT = Path(__file__).parent.parent
DATA_DIR = PROJECT_ROOT / "data"
CODE_DIR = PROJECT_ROOT / "code"

# ============================================
# RAW DATA PATHS
# ============================================
RAW_DIR = DATA_DIR / "01_raw"
CLEAN_DIR = DATA_DIR / "02_cleaned"
TEMP_DIR = DATA_DIR / "temp"

HUD_DATA = RAW_DIR / "HUD.xlsx"
HUD_CLEAN = TEMP_DIR / "HUD_clean.csv"
COC_SHP = RAW_DIR / "CoC_GIS_National_Boundary_2023/CoC_GIS_National_Boundary.gdb"
COUNTY_SHP = RAW_DIR / "tl_2023_us_county.shp"

CROSSWALK = TEMP_DIR / "CoC_county_crosswalk.csv"