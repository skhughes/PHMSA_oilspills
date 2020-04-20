*******************************************************************************
** Oil Spill Project **
** Sam Hughes **
** March 16, 2018 **

** Summarize Spill Data **
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
local hl_spill_19682017="$data"+"PHMSA/hl_spill_19682017.dta"
use "`hl_spill_19682017'",clear
** LIQUID SPILLED
replace commodity=upper(commodity)
gen crude = regexm(commodity, "CRUDE")
replace crude = regexm(commodity, "RAW") if crude==0
gen naturalgas = regexm(commodity,"NATURAL")
replace naturalgas = regexm(commodity,"LIQUID") if naturalgas==0
gen propane = regexm(commodity,"PROPANE")
replace propane = regexm(commodity,"L. P. G.") if propane==0
gen gasoline = regexm(commodity,"GASOLINE")
gen diesel = regexm(commodity,"DIESEL")
gen fuel = regexm(commodity,"FUEL")
gen volatile = regexm(commodity,"HVL")
replace volatile = regexm(commodity,"VOLATILE") if volatile==0

gen commodity_short = (crude==1)
replace commodity_short = 2 if fuel==1 & commodity_short==0
replace commodity_short = 3 if gasoline==1 & commodity_short==0
replace commodity_short = 4 if propane==1 & commodity_short==0
replace commodity_short = 5 if naturalgas==1 & commodity_short==0
replace commodity_short = 6 if diesel==1 & commodity_short==0
replace commodity_short = 7 if volatile==1 & commodity_short==0
label define commodities 0 MISC 1 CRUDE 2 FUEL 3 GASOLINE 4 PROPANE 5 "NATURAL GAS" 6 DIESEL 7 "VOLATILE LIQUID"
label values commodity_short commodities

label define commodity_short 2 FUEL,modify
label define commodity_short 3 GASOLINE,modify
label define commodity_short 4 PROPANE,modify
label define commodity_short 5 "NATURAL GAS",modify
label define commodity_short 6 DIESEL,modify
label define commodity_short 7 "VOLATILE LIQUID",modify
label define commodity_short 0 MISC,modify
**CLEAN
replace tot_fatal=0 if tot_fatal==.
replace tot_injur=0 if tot_injur==.

gen fire_ind = (fire=="YES" | fire=="Yes")
gen explosion_ind = (explosion=="YES" | explosion=="Yes")

*tabstat loss , by(year) stat(count sum mean sd min p5 p25 p50 p75 p95 max   )
*tab commodity_short
*tab fire_ind explosion_ind

gen countyfips = (st_fips*1000)+ct_fips
tostring countyfips, replace
replace countyfips = "0"+countyfips if strlen(countyfips)<5
replace countyfips = "0"+countyfips if strlen(countyfips)<5

collapse (sum) loss recovered tot_fatal tot_injur fire_ind explosion_ind cost_damage_prpty (count) numberspills=report_id, by(countyfips year)
destring countyfips, gen(fips)
local hl_countyyearspill_19682017="$data"+"PHMSA/hl_countyyearspill_19682017.dta"
save "`hl_countyyearspill_19682017'",replace

gen decade=""
forvalues decadeyear = 1960(10)2010 {
local min=`decadeyear' 
local end=`decadeyear'+10
replace decade = "`end'" if year > `min' & year<=`min'+10
}

collapse (sum) loss recovered tot_fatal tot_injur fire_ind explosion_ind cost_damage_prpty numberspills, by(countyfips decade)
*bys decade: sum
destring decade, replace
local hl_countydecadespill_19682017="$data"+"PHMSA/hl_countydecadespill_19682017.dta"
save "`hl_countydecadespill_19682017'",replace
