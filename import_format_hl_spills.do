*******************************************************************************
** Oil Spill Project **
** Sam Hughes **
** March 2, 2018 **

** Importing and formatting data files **
*******************************************************************************
*******************************************************************************
*IMPORT AND FORMAT FILES
drop _all
set more off

global data "/home/bepp/skhughes/oil_pipeline_project/data/"
global do "/home/bepp/skhughes/oil_pipeline_project/code/"
global output "/home/bepp/skhughes/oil_pipeline_project/output/"

cd "/home/bepp/skhughes/oil_pipeline_project/"
*******************************************************************************
local fips_names="$data"+"fips_state_county_names.csv"
import delimited "`fips_names'", clear
keep stabbrev st_fips ct_fips county_name
replace county_name = subinstr(county_name, ".", "",.)
tempfile fips_mergeCN
save `fips_mergeCN'
import delimited "`fips_names'", clear
keep stabbrev st_fips ct_fips county_short
duplicates drop stabbrev county_short, force
replace county_short = subinstr(county_short, ".", "",.)
tempfile fips_mergeCS
save `fips_mergeCS'
import delimited "`fips_names'", clear
keep stabbrev st_fips ct_fips censusarea_short
duplicates drop stabbrev censusarea_short, force
replace censusarea_short = subinstr(censusarea_short, ".", "",.)
tempfile fips_mergeCAS
save `fips_mergeCAS'
import delimited "`fips_names'", clear
keep stabbrev st_fips ct_fips city_short
duplicates drop stabbrev city_short, force
replace city_short = subinstr(city_short, ".", "",.)
tempfile fips_mergeCYS
save `fips_mergeCYS'
*******************************************************************************
** PHMSA **
*NOTE PROBLEMS WITH MERGES: MISSING VALUES, COUNTY/CITY WITH SAME NAME DIFFERENT FIPS
**
**1968-1986
**
local phmsa_pre1986="$data"+"PHMSA/accident_hazardous_liquid_pre1986/accident_hl_pre1986.csv"
import delimited "`phmsa_pre1986'", delimiter(comma) clear 
keep iyear report_id operator_id accident_state accident_county accident_city sys_part_involved_text ///
	origin_liquid_release_text primary_cause_text employee_fatalities non_employee_fatalities ///
	total_fatalities employee_injuries non_employee_injuries total_injuries total_prpty_damage ///
	commodity loss_bbls is_fire is_explosion
replace accident_state=upper(accident_state)
replace accident_state=strtrim(accident_state)
replace accident_county=upper(accident_county)
replace accident_county=strtrim(accident_county)

ren (accident_state accident_county ) (stabbrev county_name )
merge m:1 stabbrev county_name using "`fips_mergeCN'" , gen(_mergeCN)
drop if _mergeCN==2
ren (st_fips ct_fips) (st_fipsCN ct_fipsCN)

ren county_name county_short 
merge m:1 stabbrev county_short using "`fips_mergeCS'" , gen(_mergeCS)
drop if _mergeCS==2
ren (st_fips ct_fips) (st_fipsCS ct_fipsCS)

ren (county_short ) (censusarea_short )
merge m:1 stabbrev censusarea_short using "`fips_mergeCAS'" , gen(_mergeCAS)
drop if _mergeCAS==2
ren (st_fips ct_fips) (st_fipsCAS ct_fipsCAS)

ren (censusarea_short ) (city_short )
merge m:1 stabbrev city_short using "`fips_mergeCYS'" , gen(_mergeCYS)
drop if _mergeCYS==2
ren city_short county_name
ren (st_fips ct_fips) (st_fipsCYS ct_fipsCYS)
gen st_fips=st_fipsCN
replace st_fips=st_fipsCS if st_fips==.
replace st_fips=st_fipsCAS if st_fips==.
replace st_fips=st_fipsCYS if st_fips==.

gen ct_fips=ct_fipsCN
replace ct_fips=ct_fipsCS if ct_fips==.
replace ct_fips=ct_fipsCAS if ct_fips==.
replace ct_fips=ct_fipsCYS if ct_fips==.

replace st_fips=12 if stabbrev=="FL"
replace ct_fips=86 if county_name=="DADE" & stabbrev=="FL"
replace st_fips=29 if stabbrev=="MO"
replace ct_fips=63 if county_name=="DE KALB" & stabbrev=="MO"
replace st_fips=18 if stabbrev=="IN"
replace ct_fips=33 if county_name=="DE KALB" & stabbrev=="IN"
replace ct_fips=91 if county_name=="LA PORTE" & stabbrev=="IN"
replace st_fips=13 if stabbrev=="GA"
replace ct_fips=89 if county_name=="DE KALB" & stabbrev=="GA"
replace ct_fips=255 if county_name=="SPAULDING" & stabbrev=="GA"
replace st_fips=17 if stabbrev=="IL"
replace ct_fips=37 if county_name=="DE KALB" & stabbrev=="IL"
replace ct_fips=43 if county_name=="DU PAGE" & stabbrev=="IL"
replace st_fips=2 if stabbrev=="AK"
replace ct_fips=90 if county_name=="FAIRBANKS NORTH STAR" & stabbrev=="AK"
replace st_fips=19 if stabbrev=="IA"
replace ct_fips=141 if county_name=="O BRIEN" & stabbrev=="IA"
replace st_fips=22 if stabbrev=="LA"

ren (iyear report_id operator_id stabbrev county_name accident_city sys_part_involved_text ///
	origin_liquid_release_text commodity primary_cause_text employee_fatalities non_employee_fatalities ///
	total_fatalities employee_injuries non_employee_injuries total_injuries total_prpty_damage ///
	loss_bbls is_fire is_explosion) ///
	(year report_id operator_id state_abbrev county_name city_name system_part failed_item commodity ///
	cause emp_fatal nonemp_fatal tot_fatal emp_injur nonemp_injur tot_injur cost_damage_prpty ///
	loss fire explosion)
tempfile hl_spill_19681985
save `hl_spill_19681985'
**
**1986-2002
**
local phmsa_19862002="$data"+"PHMSA/accident_hazardous_liquid_1986_jan2002/accident_hl_1986_2002.csv"
import delimited "`phmsa_19862002'", delimiter(comma) clear 
keep rptid opid idate acstate accounty accity csys orglk caus ///
	tfat efat nfat tinj einj ninj prpty comm loss recov fire exp 
tostring idate, replace
gen iyear = substr(idate,1,4)
destring iyear, replace
replace acstate=upper(acstate)
replace acstate=strtrim(acstate)
replace accounty=upper(accounty)
replace accounty=strtrim(accounty)

ren (acstate accounty ) (stabbrev county_name )
merge m:1 stabbrev county_name using "`fips_mergeCN'" , gen(_mergeCN)
drop if _mergeCN==2
ren (st_fips ct_fips) (st_fipsCN ct_fipsCN)

ren county_name county_short 
merge m:1 stabbrev county_short using "`fips_mergeCS'" , gen(_mergeCS)
drop if _mergeCS==2
ren (st_fips ct_fips) (st_fipsCS ct_fipsCS)

ren (county_short ) (censusarea_short )
merge m:1 stabbrev censusarea_short using "`fips_mergeCAS'" , gen(_mergeCAS)
drop if _mergeCAS==2
ren (st_fips ct_fips) (st_fipsCAS ct_fipsCAS)

ren (censusarea_short ) (city_short )
merge m:1 stabbrev city_short using "`fips_mergeCYS'" , gen(_mergeCYS)
drop if _mergeCYS==2
ren city_short county_name
ren (st_fips ct_fips) (st_fipsCYS ct_fipsCYS)
gen st_fips=st_fipsCN
replace st_fips=st_fipsCS if st_fips==.
replace st_fips=st_fipsCAS if st_fips==.
replace st_fips=st_fipsCYS if st_fips==.

gen ct_fips=ct_fipsCN
replace ct_fips=ct_fipsCS if ct_fips==.
replace ct_fips=ct_fipsCAS if ct_fips==.
replace ct_fips=ct_fipsCYS if ct_fips==.
gen recov2 = loss if recov>loss
replace loss = recov if recov>loss
replace recov=recov2 if !mi(recov2)
drop recov2
ren (iyear rptid opid stabbrev county_name accity csys ///
	orglk comm caus efat nfat ///
	tfat einj ninj tinj prpty ///
	loss recov fire exp) ///
	(year report_id operator_id state_abbrev county_name city_name system_part failed_item commodity ///
	cause emp_fatal nonemp_fatal tot_fatal emp_injur nonemp_injur tot_injur cost_damage_prpty ///
	loss recovered fire explosion)

tempfile hl_spill_19862002
save `hl_spill_19862002'
**
**2002-2009
**

local phmsa_20022009="$data"+"PHMSA/accident_hazardous_liquid_jan2002_dec2009/accident_hl_2002_2009.csv"
import delimited "`phmsa_20022009'", delimiter(comma) clear 
keep iyear operator_id rptid idate acstate accounty accity ///
	prpty comm spunit_txt loss recov gen_cause_txt fail_oc_txt sysprt_txt fatal ///
	efat nfat gpfat injure einj ninj gpinj ignite explo amt_in_water 
replace acstate=upper(acstate)
replace acstate=strtrim(acstate)
replace accounty=upper(accounty)
replace accounty=strtrim(accounty)

ren (acstate accounty ) (stabbrev county_name )
merge m:1 stabbrev county_name using "`fips_mergeCN'" , gen(_mergeCN)
drop if _mergeCN==2
ren (st_fips ct_fips) (st_fipsCN ct_fipsCN)

ren county_name county_short 
merge m:1 stabbrev county_short using "`fips_mergeCS'" , gen(_mergeCS)
drop if _mergeCS==2
ren (st_fips ct_fips) (st_fipsCS ct_fipsCS)

ren (county_short ) (censusarea_short )
merge m:1 stabbrev censusarea_short using "`fips_mergeCAS'" , gen(_mergeCAS)
drop if _mergeCAS==2
ren (st_fips ct_fips) (st_fipsCAS ct_fipsCAS)

ren (censusarea_short ) (city_short )
merge m:1 stabbrev city_short using "`fips_mergeCYS'" , gen(_mergeCYS)
drop if _mergeCYS==2
ren city_short county_name
ren (st_fips ct_fips) (st_fipsCYS ct_fipsCYS)
gen st_fips=st_fipsCN
replace st_fips=st_fipsCS if st_fips==.
replace st_fips=st_fipsCAS if st_fips==.
replace st_fips=st_fipsCYS if st_fips==.

gen ct_fips=ct_fipsCN
replace ct_fips=ct_fipsCS if ct_fips==.
replace ct_fips=ct_fipsCAS if ct_fips==.
replace ct_fips=ct_fipsCYS if ct_fips==.

replace loss=loss/42 if spunit_txt=="GALLON"
replace recov=recov/42 if spunit_txt=="GALLON"
egen nonemp_fatal=rowtotal(nfat gpfat)
egen nonemp_injur=rowtotal(ninj gpinj)
ren (iyear rptid operator_id stabbrev county_name accity sysprt_txt ///
	fail_oc_txt comm gen_cause_txt efat nonemp_fatal ///
	fatal einj nonemp_injur injure prpty ///
	loss recov ignite explo) ///
	(year report_id operator_id state_abbrev county_name city_name system_part failed_item commodity ///
	cause emp_fatal nonemp_fatal tot_fatal emp_injur nonemp_injur tot_injur cost_damage_prpty ///
	loss recovered fire explosion)
tempfile hl_spill_20022009
save `hl_spill_20022009'
**
**2009-2017
**

local phmsa_20092017="$data"+"PHMSA/accident_hazardous_liquid_jan2010_present/accident_hl_2009_2017.csv"
import delimited "`phmsa_20092017'", delimiter(comma) clear 
keep iyear report_number operator_id local_datetime onshore_state_abbrev onshore_county onshore_city ///
	prpty commodity_released_type system_part_involved item_involved unintentional_release_bbls intentional_release_bbls recovered_bbls cause fatal num_emp_fatalities num_contr_fatalities num_er_fatalities num_worker_fatalities num_gp_fatalities injure num_emp_injur num_contr_injur num_er_injur num_worker_injur num_gp_injur ignite_ind explode_ind amount_released 
replace onshore_state_abbrev=upper(onshore_state_abbrev)
replace onshore_state_abbrev=strtrim(onshore_state_abbrev)
replace onshore_county=upper(onshore_county)
replace onshore_county=strtrim(onshore_county)

ren (onshore_state_abbrev onshore_county ) (stabbrev county_name )
merge m:1 stabbrev county_name using "`fips_mergeCN'" , gen(_mergeCN)
drop if _mergeCN==2
ren (st_fips ct_fips) (st_fipsCN ct_fipsCN)

ren county_name county_short 
merge m:1 stabbrev county_short using "`fips_mergeCS'" , gen(_mergeCS)
drop if _mergeCS==2
ren (st_fips ct_fips) (st_fipsCS ct_fipsCS)

ren (county_short ) (censusarea_short )
merge m:1 stabbrev censusarea_short using "`fips_mergeCAS'" , gen(_mergeCAS)
drop if _mergeCAS==2
ren (st_fips ct_fips) (st_fipsCAS ct_fipsCAS)

ren (censusarea_short ) (city_short )
merge m:1 stabbrev city_short using "`fips_mergeCYS'" , gen(_mergeCYS)
drop if _mergeCYS==2
ren city_short county_name
ren (st_fips ct_fips) (st_fipsCYS ct_fipsCYS)
gen st_fips=st_fipsCN
replace st_fips=st_fipsCS if st_fips==.
replace st_fips=st_fipsCAS if st_fips==.
replace st_fips=st_fipsCYS if st_fips==.

gen ct_fips=ct_fipsCN
replace ct_fips=ct_fipsCS if ct_fips==.
replace ct_fips=ct_fipsCAS if ct_fips==.
replace ct_fips=ct_fipsCYS if ct_fips==.

egen loss=rowtotal(intentional unintentional)
egen nonemp_fatal=rowtotal(num_contr_fatal num_er_fatal num_worker_fatal num_gp_fatal)
egen nonemp_injur=rowtotal(num_contr_injur num_er_injur num_worker_injur num_gp_injur)
ren (iyear report_number operator_id stabbrev county_name onshore_city system_part_involved ///
	item_involved commodity_released_type cause num_emp_fatal nonemp_fatal ///
	fatal num_emp_injur nonemp_injur injure prpty ///
	loss recovered_bbls ignite_ind explode_ind) ///
	(year report_id operator_id state_abbrev county_name city_name system_part failed_item commodity ///
	cause emp_fatal nonemp_fatal tot_fatal emp_injur nonemp_injur tot_injur cost_damage_prpty ///
	loss recovered fire explosion)
tempfile hl_spill_20092017
save `hl_spill_20092017'
**
**APPEND
append using "`hl_spill_20022009'"
append using "`hl_spill_19862002'"
append using "`hl_spill_19681985'"
local hl_spill_19682017="$data"+"PHMSA/hl_spill_19682017.dta"
save "`hl_spill_19682017'",replace
