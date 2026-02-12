'''
Title: master_clean.py
Author: Mary Edith Plunkett
Updated: 2026-02
'''

from clean_01_CoC_crosswalk import main as stage1
from clean_02_county_controls import main as stage2
from clean_03_policy_panel import main as stage3
from clean_04_HUD_data import main as stage4

def main():
    print("[STAGE 1] CoC-County Crosswalk")
    stage1()

    print("\n[STAGE 2] County-Level Data")
    stage2()

    print("\n[STAGE 3] State Policy Panel")
    stage3()

    print("\n[STAGE 4] HUD Data and Merging")   
    stage4()

    print("\n[ALL DONE] All cleaning stages complete!")

if __name__ == "__main__":
    main()