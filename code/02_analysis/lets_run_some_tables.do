// Title: summary_statistics.do
// Purpose: Looking for Effects of Different Types of Moratoria on Re-Housing
// Author: Mary Edith Plunkett
// Date: 02.24.26

*******************************************************
** 1.  GLOBAL PATHS AND SETTINGS
*******************************************************

clear all

// REPLACE THIS WITH YOUR PROJECT PATH
global PROJECT_ROOT "/Users/maryedithplunkett/Desktop/DMP"
global data "$PROJECT_ROOT/data/02_cleaned"
global output "$PROJECT_ROOT/output/tables/tex"
use "$data/all_data.dta"

local outcomes inflow ///
               avg_days_homeless ///
               median_days_homeless ///
               success_rate ///
               exits ///
               exits_perm

local covariates POP ///
                 UNEMP ///
                 COVID_cases ///
                 COVID_deaths

*******************************************************
** 2. OUTCOMES SUMMARY STATS
*******************************************************

eststo clear

* --------------------
* Panel A: Full Sample
* --------------------
eststo panelA: estpost tabstat `outcomes', ///
    statistics(mean sd min max n) ///
    columns(statistics)

* --------------------
* Panel B: Control
* --------------------
eststo panelB: estpost tabstat `outcomes' ///
    if overall_days == 0, ///
    statistics(mean sd min max n) ///
    columns(statistics)

* --------------------
* Panel C: Treatment
* --------------------
eststo panelC: estpost tabstat `outcomes' ///
    if overall_days != 0, ///
    statistics(mean sd min max n) ///
    columns(statistics)

* --------------------
* Export One Combined Table
* --------------------
esttab panelA panelB panelC using ///
"$output/outcomes_summary.tex", ///
cells("mean(fmt(3)) sd(fmt(3)) min(fmt(3)) max(fmt(3)) count(fmt(0))") ///
collabels("Mean" "SD" "Min" "Max" "N") ///
mtitles("Full Sample" "Control" "Treatment") ///
label nonumber noobs ///
replace

*******************************************************
** 3. COVARIATES SUMMARY STATS
*******************************************************
	
eststo clear

* --------------------
* Panel A: Full Sample
* --------------------
eststo panelA: estpost tabstat `covariates', ///
    statistics(mean sd min max n) ///
    columns(statistics)

* --------------------
* Panel B: Control
* --------------------
eststo panelB: estpost tabstat `covariates' ///
    if overall_days == 0, ///
    statistics(mean sd min max n) ///
    columns(statistics)

* --------------------
* Panel C: Treatment
* --------------------
eststo panelC: estpost tabstat `covariates' ///
    if overall_days != 0, ///
    statistics(mean sd min max n) ///
    columns(statistics)

* --------------------
* Export One Combined Table
* --------------------
esttab panelA panelB panelC using ///
"$output/covariates_summary.tex", ///
cells("mean(fmt(3)) sd(fmt(3)) min(fmt(3)) max(fmt(3)) count(fmt(0))") ///
collabels("Mean" "SD" "Min" "Max" "N") ///
mtitles("Full Sample" "Control" "Treatment") ///
label nonumber noobs ///
replace
	
