## Folder Structure
```
Data Analysis
├── Code
│   ├── admin data
│   │   ├── cleaning_code
│   │   │   ├── Task Assignment
│   │   │   └── FIRs
│   │   ├── mentor_selection
│   │   ├── posting_info
│   │   └── analysis
│   ├── randomization code
│   │   └── randomization
│   ├── SCTO 
│   │   └── endline
│   │       ├── Infrastructure
│   │       ├── Main Survey 
│   │       └── Tracking
│   ├── survey_code
│   │   ├── baseline
│   │   ├── midline
│   │   │   ├── Main Survey
│   │   │   │   ├── 1_Field_Work
│   │   │   │   │   ├── 1_Data_Flow
│   │   │   │   │   │   ├── 0_Master
│   │   │   │   │   │   ├── 1_Decrypt
│   │   │   │   │   │   ├── 2_Import
│   │   │   │   │   │   └── 3_Append
│   │   │   │   │   ├── 2_Cleaning
│   │   │   │   │   │   ├── 0_Master
│   │   │   │   │   │   └── 1_Clean
│   │   │   │   │   └── 3_HFC
│   │   │   │   │   │   ├── 0_Master
│   │   │   │   │   │   └── 1_Spread
│   │   │   │   ├── 2_Backchecks
│   │   │   │   │   ├── 1_Generate_Backcheck_Entries
│   │   │   │   │   ├── 2_Import_Backchecks
│   │   │   │   │   └── 3_Reconcile_Backchecks
│   │   │   │   ├── 3_Analysis
│   │   │   │   └── HFC logs
│   │   │   ├── Infrastructure
│   │   │   ├── Strength
│   │   │   └── Tracking Survey
│   │   ├── endline
│   │   │   ├── 1_Field_Work
│   │   │   │   ├── 1_Data_Flow
│   │   │   │   │   ├── 0_Master
│   │   │   │   │   ├── 1_Decrypt
│   │   │   │   │   ├── 2_Import
│   │   │   │   │   └── 3_Append
│   │   │   │   ├── 2_Cleaning
│   │   │   │   │   ├── 0_Master
│   │   │   │   │   └── 1_Clean
│   │   │   │   └── 3_HFC
│   │   │   │   │   ├── 0_Master
│   │   │   │   │   └── 1_Spread
│   │   │   ├── 2_Backchecks
│   │   │   │   ├── 1_Generate_Backcheck_Entries
│   │   │   │   ├── 2_Import_Backchecks
│   │   │   │   └── 3_Reconcile_Backchecks
│   │   │   ├── 3_Analysis
│   │   │   └── HFC_logs
│   │   ├── pooled
│   │   ├── ims_short
│   │   │   ├── ims calls
│   │   │   └── ims in_person
│   │   ├── survey_nbd_districts
│   │   ├── programs
│   │   └── task assignment
│   └── urja_rct
│       └── graphs_catchement_area_rct_UJRA
├── Data
│   ├── admin data
│   │   ├── Marks_2022 Batch
│   │   │   ├── pdf
│   │   │   └── Excel
│   │   ├── mentors_selection
│   │   │   ├── clean_data
│   │   │   └── raw_data
│   │   ├── police_station_char
│   │   │   ├── Excel
│   │   │   └── Stata
│   │   ├── posting_info
│   │   │   ├── clean_data
│   │   │   ├── July 2024 mismatch .do
│   │   │   └── raw_data
│   │   │       ├── Excel
│   │   │       └── Stata
│   │   ├── previous rounds of recruiting
│   │   ├── recruitment_data
│   │   │   ├── clean_data
│   │   │   ├── raw_data
│   │   │   └── semi_clean_data
│   │   ├── CCTNS (duplicate of Monthly FIR?)
│   │   │   ├── CLEAN
│   │   │   │   └── by_ps
│   │   │   └── RAW
│   │   ├── DIR
│   │   ├── Task Assignment
│   │   │   ├── balaghat_dta
│   │   │   ├── betul_dta
│   │   │   ├── chhatarpur_dta
│   │   │   ├── panna_dta
│   │   │   ├── ratlam_dta
│   │   │   ├── rewa_dta
│   │   │   ├── shahdol_dta
│   │   │   ├── tikamgarh_dta
│   │   │   ├── ujjain_dta
│   │   │   ├── raisen_dta
│   │   │   ├── satna_dta
│   │   │   ├── dewas_dta
│   │   │   ├── mandsaur_dta
│   │   │   ├── rajgarh_dta
│   │   │   ├── shajhapur_dta
│   │   │   ├── bhind_dta
│   │   │   └── sagar_dta
│   │   ├── Annual FIR
│   │   ├── Current Posting - 2026
│   │   ├── Monthly FIR
│   │   │   ├── raw
│   │   │   └── clean
│   │   └── URJA implementation
│   │       ├── raw
│   │       └── clean
│   ├── census
│   │   ├── census2001
│   │   │   └── shapefiles
│   │   │       └── Districts
│   │   └── census2011
│   │       └── shapefiles
│   │           ├── Districts
│   │           └── geoBoundaries-IND-ADM2-all
│   ├── India Justice Report data
│   ├── montoring
│   ├── NFHC 5 2019-2021
│   │   ├── GeographicCovariates
│   │   └── ShapeFiles
│   ├── randomization data
│   │   └── randomization
│   ├── shrug_data
│   │   ├── shrug-pc11-village-poly-shp
│   │   ├── shrug-pc11dist-poly-shp
│   │   ├── shrug-pca11-dta
│   │   ├── shrug-shrid-keys-dta
│   │   └── shrug-shrid-poly-shp
│   ├── survey_data (get rid of police_station_size etc.)
│   │   ├── baseline (get rid of patch)
│   │   │   └── 2_Without_PII 
│   │   ├── midline
│   │   │   ├── 1_Veracrypt
│   │   │   └── 2_Without_PII
│   │   │       ├── Clean
│   │   │       ├── Raw  
│   │   │       ├── Merged
│   │   │       ├── Infrastructure
│   │   │       │   ├── Cleaned 
│   │   │       │   └── Raw
│   │   │       ├── Tracking
│   │   │       │   ├── Cleaned 
│   │   │       │   └── Raw
│   │   │       └── Strength
│   │   │           ├── Cleaned 
│   │   │           └── Raw
│   │   ├── endline
│   │   │   ├── 1_Veracrypt
│   │   │   └── 2_Without_PII
│   │   │       ├── Raw
│   │   │       └── Cleaned
│   │   ├── pooled
│   │   ├── survey_nbd_districts
│   │   │   ├── clean_data
│   │   │   └── raw_data
│   │   └── ims_short
│   │       ├── ims calls
│   │       │   ├── clean_data
│   │       │   └── raw_data
│   │       └── ims in_person
│   │           ├── clean_data
│   │           └── raw_data
│   ├── urja_rct
│   │   ├── census_info_rct_ps
│   │   │   ├── polygod_rct_dist
│   │   │   └── rct_census
│   │   ├── GIS
│   │   ├── ims_scale_up
│   │   ├── merged_data
│   │   │   ├── merge_2
│   │   │   ├── merge_3
│   │   │   └── merge_4
│   │   └── population
│   └── Data Backup
└── Output
    ├── graphs (clean this)
    │   ├── urja-rct
    │   │   ├── jurisdiction_district
    │   │   ├── jusrisdiction_random_ps
    │   │   └── population_admin_census
    │   ├── station characteristics
    │   ├── challenges
    │   ├── false reports
    │   ├── aspirations
    │   └── FIRs
    └── tables
        ├── baseline
        │   └── fragments (rename tex?)
        ├── midline
        │   └── tex
        ├── endline
        │   └── tex
        ├── pooled
        │   └── tex 
        ├── qrsampling (maps of sample districts)
        ├── IMS
        └── NBD
```
