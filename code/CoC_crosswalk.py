import geopandas as gpd
import fiona
from pathlib import Path
from config import COC_SHP, COUNTY_SHP, COUNTY_CLEAN
import os

def loading_and_cleaning():
    import os
    print("Exists:", os.path.exists(COC_SHP))
    print("Is dir:", os.path.isdir(COC_SHP))
    layers = fiona.listlayers(COC_SHP)
    print("Loading data...")

    coc = gpd.read_file(COC_SHP, layer=layers[0], driver="OpenFileGDB").to_crs(epsg=5070)
    cty = gpd.read_file(COUNTY_SHP).to_crs(5070)
    print("Cleaning data...")
    keep_cols_coc = ["STATE_NAME", "COCNUM", "COCNAME", "geometry"]
    coc_clean = coc[keep_cols_coc].copy()
    # dissolving by same CoC to make sure each CoC is a single polygon
    coc_clean = coc_clean.dissolve(
        by=["STATE_NAME", "COCNUM", "COCNAME"],
        as_index=False
    )

    keep_cols_cty = ['STATEFP', 'COUNTYFP', 'NAME', 'GEOID', 'geometry']
    cty_clean = cty[keep_cols_cty].copy()
    cty_clean["county_fips5"] = cty_clean["GEOID"].astype(str).str.zfill(5)
    cty_clean["statefips"] = cty_clean["county_fips5"].str[:2]
    cty_clean["countyfips"] = cty_clean["county_fips5"].str[2:]

    return coc_clean, cty_clean

def overlay(coc, cty):
    # ---- Overlay intersections ----
    print("[INFO] Computing intersections (county x CoC). This can take a bit.")
    inter = gpd.overlay(
        cty[["county_fips5", "statefips", "countyfips", "geometry"]],
        coc[["COCNUM", "geometry"]],
        how="intersection"
    )

    if len(inter) == 0:
        # Helpful debugging info
        print("[ERROR] Overlay returned 0 intersections.")
        print("CoC bounds:", coc.total_bounds)
        print("County bounds:", cty.total_bounds)
        raise RuntimeError("No intersections found. Likely CRS or geometry mismatch.")

    inter["int_area"] = inter.geometry.area

    # ---- Choose 1-to-1 mapping: largest overlap area ----
    # For each county, keep the CoC with max intersection area
    inter_sorted = inter.sort_values(["county_fips5", "int_area"], ascending=[True, False])
    best = inter_sorted.groupby("county_fips5", as_index=False).first()

    # ---- Output exactly the columns you want ----
    out = best[["statefips", "countyfips", "COCNUM"]].copy()
    out = out.rename(columns={"COCNUM": "coc_id"})

    # Sanity checks
    print("[INFO] Output rows:", len(out))
    print("[INFO] Unique counties:",
          out["statefips"].astype(str).str.zfill(2).add(out["countyfips"].astype(str).str.zfill(3)).nunique())
    print("[INFO] Example rows:\n", out.head())

    # Save
    out.to_csv(COUNTY_CLEAN, index=False)
    print(f"[DONE] Wrote crosswalk to: {COUNTY_CLEAN}")

def main():
    coc, cty = loading_and_cleaning()
    overlay(coc, cty)
    print("[DONE]")

if __name__ == "__main__":
    main()