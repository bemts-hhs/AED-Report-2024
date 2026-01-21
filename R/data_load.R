###_____________________________________________________________________________
# Prepare Data for Analysis
# Run this script first before moving on to analyses
###_____________________________________________________________________________

# Get environment variables into the global environment
aed_data_path <- Sys.getenv("aed_env")
ems_data_path <- Sys.getenv("ems_data_env")
iowa_county_district_path <- Sys.getenv("iowa_county_district_env")
output_folder <- Sys.getenv("output_directory")

###_____________________________________________________________________________
# read in the data
###_____________________________________________________________________________

# raw EMS data for comparison
ems_raw <- readr::read_csv(
  ems_data_path
) |>
  janitor::clean_names(case = "title", sep_out = "_")

# clean the EMS data for comparison
ems_aed <- ems_raw |>
  dplyr::mutate(
    Incident_Date = lubridate::mdy(stringr::str_remove(
      Incident_Date,
      pattern = "12:00:00 AM"
    )),
    dplyr::across(
      matches("date_time"),
      ~ lubridate::mdy_hms(stringr::str_remove(., pattern = "\\sAM|\\sPM"))
    ),
    Incident_Dispatch_Notified_to_Unit_Arrived_on_Scene_in_Minutes = as.numeric(
      difftime(
        Incident_Unit_Arrived_on_Scene_Date_Time_e_Times_06,
        Incident_Unit_Notified_by_Dispatch_Date_Time_e_Times_03,
        units = "mins"
      )
    ),
    Incident_Dispatch_Notified_to_Unit_Arrived_at_Patient_in_Minutes = as.numeric(
      difftime(
        Incident_Unit_Arrived_at_Patient_Date_Time_e_Times_07,
        Incident_Unit_Notified_by_Dispatch_Date_Time_e_Times_03,
        units = "mins"
      )
    ),
    Cardiac_Arrest_Who_Provided_Cpr_Prior_to_Ems_Arrival_List_3_4_e_Arrest_06_3_5_it_Arrest_106 = stringr::str_remove_all(
      Cardiac_Arrest_Who_Provided_Cpr_Prior_to_Ems_Arrival_List_3_4_e_Arrest_06_3_5_it_Arrest_106,
      pattern = "\""
    )
  ) |>
  dplyr::mutate(
    Unique_Run_ID = stringr::str_c(
      Agency_Name_d_Agency_03,
      Incident_Patient_Care_Report_Number_Pcr_e_Record_01,
      Incident_Date,
      sep = "-"
    ),
    .before = 1
  )

###_____________________________________________________________________________
# Analyze EMS data
###_____________________________________________________________________________

# get all unique procedures, examine
all_procedures <- ems_aed |>
  dplyr::select(Procedure_Performed_Description_e_Procedures_03) |>
  dplyr::distinct() |>
  dplyr::arrange(Procedure_Performed_Description_e_Procedures_03)

# get patients with defibrillation procedures
# select only procedure, date/time, and # attempts
# get the unique run IDs with defib using filter
# get the dplyr::distinct rows which will be unique run ID, procedure, date/time, and # of shocks, so you have each row as a dplyr::distinct procedure with date/time and # shocks
# take the sum of the procedures per Run ID which gives you # of shocks total over all defib procedures

ems_aed_defib <- ems_aed |>
  dplyr::select(
    Fact_Incident_Pk,
    Procedure_Performed_Description_e_Procedures_03,
    Procedure_Performed_Date_Time_e_Procedures_01,
    Procedure_Number_of_Attempts_e_Procedures_05
  ) |>
  dplyr::filter(
    grepl(
      pattern = "Automatic external cardiac defibrillator (physical object)|CV - Automated External Defibrillator|Automatic cardiac defibrillator (physical object)|CV - Defibrillation - Manual|Defibrillation, AED|Electrical cardioversion (& defibrillation)|CV - Cardioversion|Cardiac resuscitation",
      x = Procedure_Performed_Description_e_Procedures_03,
      ignore.case = T
    )
  ) |>
  dplyr::distinct() |>
  tidyr::replace_na(list(Procedure_Number_of_Attempts_e_Procedures_05 = 1)) |>
  dplyr::group_by(Fact_Incident_Pk) |>
  dplyr::mutate(
    Shocks = sum(Procedure_Number_of_Attempts_e_Procedures_05, na.rm = T),
    across(
      Procedure_Performed_Description_e_Procedures_03:Procedure_Number_of_Attempts_e_Procedures_05,
      ~ stringr::str_c(., collapse = ", ")
    )
  ) |>
  dplyr::distinct()

# just get the shocks as a separate object for the join
ems_shocks <- ems_aed_defib |>
  dplyr::select(Fact_Incident_Pk, Shocks)

# read in location data for regions / urbanicity
location <- readxl::read_excel(
  iowa_county_district_path
)

# deal with multiple procedures to reduce the rows to 1 row = 1 run, no duplication
ems_aed_runs <- ems_aed |>
  dplyr::mutate(
    Incident_Day = lubridate::wday(Incident_Date, label = T, abbr = F),
    Weekday_Weekend = traumar::weekend(Incident_Date),
    Season = traumar::season(Incident_Date),
    .after = Incident_Date_Time
  ) |>
  dplyr::group_by(Fact_Incident_Pk) |>
  dplyr::mutate(dplyr::across(
    c(
      Incident_Complaint_Reported_by_Dispatch_Dispatch_Reason_e_Dispatch_01,
      Situation_Provider_Primary_Impression_Description_Only_e_Situation_11,
      Procedure_Performed_Description_e_Procedures_03,
      Procedure_Performed_Date_Time_e_Procedures_01,
      Medication_Given_or_Administered_Description_e_Medications_03,
      Medication_Response_e_Medications_07
    ),
    ~ stringr::str_c(unique(.), collapse = ", ")
  )) |>
  dplyr::mutate(
    Procedure_Number_of_Attempts_e_Procedures_05 = stringr::str_c(
      Procedure_Number_of_Attempts_e_Procedures_05,
      collapse = ", "
    )
  ) |>
  dplyr::mutate(
    Witnessed = !grepl(
      pattern = "not",
      x = Cardiac_Arrest_Witnessed_by_List_e_Arrest_04,
      ignore.case = T
    ),
    .after = Cardiac_Arrest_Witnessed_by_List_e_Arrest_04
  ) |>
  naniar::replace_with_na(
    replace = list(
      Cardiac_Arrest_Cpr_Provided_Prior_to_Ems_Arrival_3_4_e_Arrest_05_3_5_it_Arrest_105 = c(
        "Not Recorded",
        "Not Applicable"
      )
    )
  ) |>
  dplyr::mutate(
    Cardiac_Arrest_Cpr_Provided_Prior_to_Ems_Arrival_3_4_e_Arrest_05_3_5_it_Arrest_105 = dplyr::if_else(
      is.na(
        Cardiac_Arrest_Cpr_Provided_Prior_to_Ems_Arrival_3_4_e_Arrest_05_3_5_it_Arrest_105
      ) &
        (!is.na(
          Cardiac_Arrest_Who_Initiated_Cpr_3_4_it_Arrest_008_3_5_e_Arrest_20
        ) &
          !Cardiac_Arrest_Who_Initiated_Cpr_3_4_it_Arrest_008_3_5_e_Arrest_20 %in%
            c("Responding EMS Personnel", "First Responder (EMS)")) |
        !is.na(
          Cardiac_Arrest_Who_Provided_Cpr_Prior_to_Ems_Arrival_List_3_4_e_Arrest_06_3_5_it_Arrest_106
        ),
      "Yes",
      Cardiac_Arrest_Cpr_Provided_Prior_to_Ems_Arrival_3_4_e_Arrest_05_3_5_it_Arrest_105
    ),
    Cardiac_Arrest_Cpr_Provided_Prior_to_Ems_Arrival_3_4_e_Arrest_05_3_5_it_Arrest_105 = tidyr::replace_na(
      Cardiac_Arrest_Cpr_Provided_Prior_to_Ems_Arrival_3_4_e_Arrest_05_3_5_it_Arrest_105,
      replace = "No"
    ),
    .after = Cardiac_Arrest_Cpr_Provided_Prior_to_Ems_Arrival_3_4_e_Arrest_05_3_5_it_Arrest_105
  ) |>
  dplyr::ungroup() |>
  dplyr::full_join(ems_shocks, by = dplyr::join_by(Fact_Incident_Pk)) |>
  dplyr::relocate(
    Shocks,
    .after = Procedure_Number_of_Attempts_e_Procedures_05
  ) |>
  dplyr::distinct(Fact_Incident_Pk, .keep_all = T) |>
  tidyr::replace_na(list(Situation_Possible_Overdose = FALSE)) |>
  dplyr::mutate(
    dplyr::across(
      c(
        Incident_Dispatch_Notified_to_Unit_Arrived_at_Patient_in_Minutes,
        Incident_Dispatch_Notified_to_Unit_Arrived_on_Scene_in_Minutes
      ),
      ~ traumar::impute(., focus = "missing", method = "median")
    ),
    dplyr::across(
      c(
        Incident_Dispatch_Notified_to_Unit_Arrived_at_Patient_in_Minutes,
        Incident_Dispatch_Notified_to_Unit_Arrived_on_Scene_in_Minutes
      ),
      ~ traumar::impute(
        .,
        focus = "skew",
        method = "winsorize",
        percentile = 0.95
      )
    )
  ) |>
  dplyr::mutate(
    Cardiac_Arrest_Patient_Outcome_at_End_of_Ems_Event_e_Arrest_18 = dplyr::if_else(
      is.na(
        Cardiac_Arrest_Patient_Outcome_at_End_of_Ems_Event_e_Arrest_18
      ) |
        Cardiac_Arrest_Patient_Outcome_at_End_of_Ems_Event_e_Arrest_18 %in%
          c("Not Applicable", "Not Recorded") &
          grepl(
            pattern = "dead",
            x = Disposition_Incident_Patient_Disposition_3_4_e_Disposition_12_3_5_it_Disposition_112,
            ignore.case = T
          ),
      "Expired in the Field",
      Cardiac_Arrest_Patient_Outcome_at_End_of_Ems_Event_e_Arrest_18
    )
  ) |>
  dplyr::mutate(
    ROSC_ED = dplyr::case_when(
      grepl(
        pattern = "rosc in the ed",
        x = Cardiac_Arrest_Patient_Outcome_at_End_of_Ems_Event_e_Arrest_18,
        ignore.case = T
      ) ~ TRUE,
      TRUE ~ FALSE
    ),
    ROSC_Field = dplyr::case_when(
      grepl(
        pattern = "rosc in the field",
        x = Cardiac_Arrest_Patient_Outcome_at_End_of_Ems_Event_e_Arrest_18,
        ignore.case = T
      ) ~ TRUE,
      TRUE ~ FALSE
    ),
    Ongoing_Resus_EMS = dplyr::case_when(
      grepl(
        pattern = "Ongoing Resuscitation by Other EMS",
        x = Cardiac_Arrest_Patient_Outcome_at_End_of_Ems_Event_e_Arrest_18,
        ignore.case = T
      ) ~ TRUE,
      TRUE ~ FALSE
    ),
    Ongoing_Resus_ED = dplyr::case_when(
      grepl(
        pattern = "Ongoing Resuscitation in ED",
        x = Cardiac_Arrest_Patient_Outcome_at_End_of_Ems_Event_e_Arrest_18,
        ignore.case = T
      ) ~ TRUE,
      TRUE ~ FALSE
    ),
    Expire_Field = dplyr::case_when(
      grepl(
        pattern = "expired in the field",
        x = Cardiac_Arrest_Patient_Outcome_at_End_of_Ems_Event_e_Arrest_18,
        ignore.case = T
      ) ~ TRUE,
      TRUE ~ FALSE
    ),
    Expire_ED = dplyr::case_when(
      grepl(
        pattern = "expired in the ed",
        x = Cardiac_Arrest_Patient_Outcome_at_End_of_Ems_Event_e_Arrest_18,
        ignore.case = T
      ) ~ TRUE,
      TRUE ~ FALSE
    ),
    Survival = dplyr::case_when(
      grepl(
        pattern = "expire",
        x = Cardiac_Arrest_Patient_Outcome_at_End_of_Ems_Event_e_Arrest_18,
        ignore.case = T
      ) ~ FALSE,
      TRUE ~ TRUE
    ),
    .after = Cardiac_Arrest_Patient_Outcome_at_End_of_Ems_Event_e_Arrest_18
  ) |>
  dplyr::left_join(
    location,
    by = c("Scene_Incident_County_Name_e_Scene_21" = "County")
  )

###_____________________________________________________________________________
# export the EMS cardiac arrest event data to .csv for analyses in Tableau
###_____________________________________________________________________________

readr::write_csv(
  x = ems_aed_runs,
  file = file.path(output_folder, "ems_aed_runs.csv")
)

###_____________________________________________________________________________
# continue reading in data, AED analyses, manipulations
###_____________________________________________________________________________

# raw AED data
aed_raw <-
  readr::read_csv(
    aed_data_path,
    col_types = list(
      readr::col_date(format = "%Y-%m-%d"),
      #1
      readr::col_time(),
      #2
      readr::col_time(),
      #3
      readr::col_time(),
      #4
      readr::col_time(),
      #5
      readr::col_character(),
      #6
      readr::col_number(),
      #7
      readr::col_skip(),
      #8
      readr::col_skip(),
      #9
      readr::col_skip(),
      #10
      readr::col_skip(),
      #11
      readr::col_character(),
      #12
      readr::col_skip(),
      #13
      readr::col_character(),
      #14
      readr::col_logical(),
      #15
      readr::col_logical(),
      #16
      readr::col_character(),
      #17
      readr::col_logical(),
      #18
      readr::col_logical(),
      #19
      readr::col_logical(),
      #20
      readr::col_logical(),
      #21
      readr::col_number(),
      #22
      readr::col_factor(),
      #23
      readr::col_factor(),
      #24
      readr::col_logical(),
      #25
      readr::col_logical(),
      #26
      readr::col_logical(),
      #27
      readr::col_logical(),
      #28
      readr::col_logical(),
      #29
      readr::col_skip(),
      #30
      readr::col_logical(),
      #31
      readr::col_number(),
      #32
      readr::col_number(),
      #33
      readr::col_number(),
      #34
      readr::col_number(),
      #35
      readr::col_number(),
      #36
      readr::col_number(),
      #37
      readr::col_character() #38
    )
  ) |>
  janitor::clean_names(case = "all_caps")

# read in Iowa county and region data
counties_regions <-
  readxl::read_excel(
    iowa_county_district_path
  )

# read in the zipcode data to get Iowa town names
zipcodeR::download_zip_data()

# get the zipcodes into memory
zip_code_db <- zipcodeR::zip_code_db

zipcodes <- zipcodeR::search_state("IA")

# remove intermediate object
rm(zip_code_db)
gc()

# unzip the larger US data from Geonames

# create a temp file to hold the download as a container
temp <- tempfile()

# download the temp file
download.file("https://download.geonames.org/export/dump/US.zip", temp)

# unzip the zip file and dplyr::pull the specific flat file
con <- unz(temp, "US.txt")

# read in the file of interest and specify column names and types using readr
US <-
  readr::read_delim(
    file = con,
    col_names = FALSE,
    col_types = list(
      readr::col_character(),
      readr::col_character(),
      readr::col_character(),
      readr::col_character(),
      readr::col_number(),
      readr::col_number(),
      readr::col_character(),
      readr::col_character(),
      readr::col_character(),
      readr::col_character(),
      readr::col_character(),
      readr::col_character(),
      readr::col_character(),
      readr::col_character(),
      readr::col_integer(),
      readr::col_integer(),
      readr::col_guess(),
      readr::col_character(),
      readr::col_date()
    )
  )

# remove the temp file
unlink(temp)

###_____________________________________________________________________________
# manipulate the US file, filter down to Iowa, and get formatting right
# for colnames, refer to: https://download.geonames.org/export/dump/readme.txt
###_____________________________________________________________________________

US_clean <- US |>
  dplyr::rename(
    geonameid = X1,
    name = X2,
    asciiname = X3,
    alternatenames = X4,
    latitude = X5,
    longitude = X6,
    feature_class = X7,
    feature_code = X8,
    country_code = X9,
    cc2 = X10,
    admin1_code = X11,
    admin2_code = X12,
    admin3_code = X13,
    admin4_code = X14,
    population = X15,
    elevation = X16,
    dem = X17,
    timezone = X18,
    modification_date = X19
  ) |>
  dplyr::filter(admin1_code == "IA", feature_class == "P") |>
  dplyr::mutate(
    name = stringr::str_squish(stringr::str_remove(
      name,
      pattern = "\\s\\(historical\\)"
    ))
  ) # remove the (historical) suffix to the "abandonded" populated places

###_____________________________________________________________________________
# load in geonames county-level data
###_____________________________________________________________________________

geonames_county <-
  readr::read_delim(
    file = "https://download.geonames.org/export/dump/admin2Codes.txt",
    col_names = F
  ) |>
  dplyr::rename(
    code = X1,
    name = X2,
    asciiname = X3,
    geonameID = X4
  ) |>
  dplyr::mutate(
    country_code = stringr::str_split(code, "\\.", simplify = T)[, 1],
    state_code = stringr::str_split(code, "\\.", simplify = T)[, 2],
    county_code = stringr::str_split(code, "\\.", simplify = T)[, 3],
    .after = code
  )

###_____________________________________________________________________________
# geonames admin2 data filtered data to US and Iowa
###_____________________________________________________________________________

geonames_admin2_iowa <- geonames_county |>
  dplyr::filter(country_code == "US", state_code == "IA")

###_____________________________________________________________________________
# join the county names / codes from geonames to the city data from geonames as their codes are unique
# geonames does not use the same codes as Census Bureau
###_____________________________________________________________________________

# final manipulations of the Iowa data to join to the AED data

Iowa_Data_Final <- US_clean |>
  dplyr::left_join(
    geonames_admin2_iowa |> dplyr::select(county_code, name),
    by = c("admin2_code" = "county_code"),
    suffix = c("_city", "_county")
  ) |>
  dplyr::relocate(name_county, .after = name_city) |>
  dplyr::mutate(
    name_county = stringr::str_remove_all(
      name_county,
      pattern = "\\s[Cc]ounty"
    ),
    name_city = stringr::str_to_upper(name_city)
  ) |>
  dplyr::filter(
    !(name_city == "CENTERVILLE" & name_county == "Boone") &
      !(name_city == "PLEASANT HILL" &
        name_county == "Van Buren") &
      !(name_city == "HOLY CROSS" &
        name_county == "Delaware") &
      !(name_city == "FOREST CITY" &
        name_county == "Howard") &
      !(name_city == "GENEVA" & name_county == "Benton") &
      !(name_city == "WESTFIELD" &
        name_county == "Poweshiek") &
      !(name_city == "TROY" & name_county == "Lucas") &
      !(name_city == "WASHINGTON" &
        name_county == "Woodbury") &
      !(name_city == "WEBSTER" & name_county == "Madison") &
      !(name_city == "RIVERSIDE" &
        name_county == "Woodbury") &
      !(name_city == "WASHINGTON" &
        name_county == "Franklin") &
      !(name_city == "VAN")
  ) |>
  dplyr::left_join(
    counties_regions |>
      dplyr::select(County, `Region: Preparedness`, Designation),
    by = c("name_county" = "County")
  ) |>
  dplyr::relocate(`Region: Preparedness`, .after = name_county)

# geonames is missing some of the cities / towns in Iowa, this is a product
# of the analyses below and may need to be revised periodically

missing_location_data <- tibble::tibble(
  geonameid = generate_random_ID(7),
  # Generating random geoname IDs
  name_city = c(
    "TERRILL",
    "LEMARS",
    "FREDRICKSBURG",
    "MOLVILLE",
    "CALLENDAR",
    "SYDNEY",
    "DESOTO"
  ),
  name_county = c(
    "Dickinson",
    "Plymouth",
    "Chickasaw",
    "Woodbury",
    "Webster",
    "Fremont",
    "Dallas"
  ),
  latitude = c(
    43.305473,
    42.7942,
    42.964586,
    42.488210,
    42.362592,
    40.7592,
    41.5316
  ),
  longitude = c(
    -94.971433,
    -96.1656,
    -92.198465,
    -96.069997,
    -94.293268,
    -95.6668,
    -94.0078
  ),
  population = rep(NA, 7),
  elevation = rep(NA, 7),
  dem = rep(NA, 7),
  timezone = rep(NA_character_, 7),
  modification_date = rep(NA, 7),
  `Region: Preparedness` = c("7", "3", "2", "3", "7", "4", "1A"),
  # Adding region information
  asciiname = rep(NA_character_, 7),
  alternatenames = rep(NA_character_, 7),
  feature_class = rep(NA_character_, 7),
  feature_code = rep(NA_character_, 7),
  country_code = rep(NA_character_, 7),
  cc2 = rep(NA_character_, 7),
  admin1_code = rep(NA_character_, 7),
  admin2_code = c("059", "149", "037", "193", "187", "071", "049"),
  # Adding admin2_code
  admin3_code = rep(NA_character_, 7),
  admin4_code = rep(NA_character_, 7)
) |>
  dplyr::left_join(
    counties_regions |> dplyr::select(County, Designation),
    by = c("name_county" = "County")
  ) |>
  dplyr::select(colnames(Iowa_Data_Final))


Iowa_Data_Final <- dplyr::bind_rows(Iowa_Data_Final, missing_location_data)

###_____________________________________________________________________________
# Exploratory data analysis indicates need for manipulation of some of the fields
###_____________________________________________________________________________

# Explore the observed distribution of the TIME_FROM_CALL_TO_PATIENT column
aed_raw |>
  dplyr::mutate(
    Year = lubridate::year(DATE_OF_USE),
    TIME_FROM_CALL_TO_PATIENT = abs(TIME_FROM_CALL_TO_PATIENT)
  ) |>
  dplyr::filter(Year >= 2022, TIME_FROM_CALL_TO_PATIENT < 100) |>
  traumar::is_it_normal(
    TIME_FROM_CALL_TO_PATIENT,
    group_vars = "Year",
    normality_test = "ad",
    include_plots = T
  )

# there are some negative times that do not make sense in aed_raw$`Time from call to patient`
# find those cases and try to fix

times_less_than_zero <- aed_raw |>
  dplyr::filter(TIME_FROM_CALL_TO_PATIENT < 0)

# check to see if LEOs turned on the AED at or after midnight when the call came in during 2300-2359
weird_time_difference <- aed_raw |>
  dplyr::filter(
    lubridate::hour(AMB_TOC) == 23 & lubridate::hour(TIME_AED_ON) == 0
  )

###_____________________________________________________________________________
# cleaning the data, some manipulations for further analysis
###_____________________________________________________________________________

aed_clean <- aed_raw |>
  dplyr::mutate(
    UNIQUE_INCIDENT_ID = generate_random_ID(
      n = nrow(aed_raw),
      set_seed = 12345
    ),
    .before = 1
  ) |>
  dplyr::mutate(
    dplyr::across(
      AMB_TOC:TIME_AED_ON,
      ~ lubridate::make_datetime(
        year = lubridate::year(DATE_OF_USE),
        month = lubridate::month(DATE_OF_USE),
        day = lubridate::day(DATE_OF_USE),
        hour = lubridate::hour(.),
        min = lubridate::minute(.),
        sec = lubridate::second(.) # create datetime objects to do date math
      )
    ),
    LOCATION_CITY_EVENT = dplyr::if_else(
      grepl(
        pattern = "Iowa State Fair Grounds",
        x = LOCATION_CITY_EVENT,
        ignore.case = TRUE
      ),
      "Des Moines",
      LOCATION_CITY_EVENT
    ),
    AMB_PAT = dplyr::if_else(
      lubridate::hour(AMB_TOC) == 23 &
        lubridate::hour(AMB_PAT) == 0,
      AMB_PAT + lubridate::days(1),
      AMB_PAT
    ),
    # fix some of the dates on the time to patient data so the math is right
    TIME_FROM_CALL_TO_PATIENT = as.numeric(difftime(
      AMB_PAT,
      AMB_TOC,
      units = "mins"
    )),
    TIME_FROM_CALL_TO_AED_ON = as.numeric(difftime(
      TIME_AED_ON,
      AMB_TOC,
      units = "mins"
    )),
    TIME_FROM_PATIENT_TO_AED = as.numeric(difftime(
      TIME_AED_ON,
      AMB_PAT,
      units = "mins"
    ))
  ) |>
  dplyr::mutate(
    TIME_AED_OFF = TIME_AED_ON + TOTAL_TIME_AED_ACTUALLY_USED_MIN_SEC,
    .after = TIME_AED_ON
  ) |>
  dplyr::mutate(
    TOTAL_TIME_AED_USED = as.numeric(difftime(
      TIME_AED_OFF,
      TIME_AED_ON,
      units = "mins"
    )),
    TIME_AT_PATIENT_TO_END_AED = as.numeric(difftime(
      TIME_AED_OFF,
      AMB_PAT,
      units = "mins"
    )),
    TIME_FROM_CALL_TO_END_AED = as.numeric(difftime(
      TIME_AED_OFF,
      AMB_TOC,
      units = "mins"
    )),
    UNKNOWN_IF_WITNESSED = dplyr::if_else(
      is.na(UNKNOWN_IF_WITNESSED),
      TRUE,
      UNKNOWN_IF_WITNESSED
    ),
    BYSTANDER_CPR = dplyr::if_else(is.na(BYSTANDER_CPR), FALSE, BYSTANDER_CPR),
    SURVIVAL = dplyr::if_else(RESULT_PATIENT_EXPIRED == TRUE, FALSE, TRUE),
    UTSTEIN_SURVIVAL = dplyr::if_else(
      BYSTANDER_CPR == TRUE &
        WITNESSED == TRUE &
        SHOCK_NO_SHOCK > 0,
      TRUE,
      FALSE
    )
  ) |>
  dplyr::mutate(
    AGE_UNIT = dplyr::if_else(
      grepl(
        pattern = "days",
        x = AGE_UNIT,
        ignore.case = T
      ),
      "Days",
      dplyr::if_else(
        grepl(
          pattern = "months",
          x = AGE_UNIT,
          ignore.case = T
        ),
        "Months",
        dplyr::if_else(
          grepl(
            pattern = "years",
            x = AGE_UNIT,
            ignore.case = T
          ),
          "Years",
          dplyr::if_else(
            grepl(
              pattern = "weeks",
              x = AGE_UNIT,
              ignore.case = T
            ),
            "Weeks",
            AGE_UNIT
          )
        )
      )
    ),
    AGE_UNIT = factor(AGE_UNIT, levels = c("Years", "Months", "Weeks", "Days")),
    AGE_YEARS = dplyr::if_else(
      grepl(
        pattern = "days",
        x = AGE_UNIT,
        ignore.case = T
      ),
      AGE / 365,
      dplyr::if_else(
        grepl(
          pattern = "years",
          x = AGE_UNIT,
          ignore.case = T
        ),
        AGE,
        dplyr::if_else(
          grepl(
            pattern = "months",
            x = AGE_UNIT,
            ignore.case = T
          ),
          AGE / 12,
          dplyr::if_else(
            grepl(
              pattern = "weeks",
              x = AGE_UNIT,
              ignore.case = T
            ),
            AGE / 52,
            AGE
          )
        )
      )
    ),
    AGE_RANGE = dplyr::case_when(
      AGE_YEARS >= 0 & AGE_YEARS < 18 ~ "0 - 17",
      AGE_YEARS >= 18 &
        AGE_YEARS < 35 ~ "18 - 34",
      AGE_YEARS >= 35 & AGE_YEARS < 65 ~ "35 - 64",
      AGE_YEARS >= 65 ~ "65+",
      TRUE ~ "Missing"
    ),
    .after = AGE
  ) |>
  dplyr::mutate(
    AGENCY = stringr::str_squish(AGENCY),
    AGENCY_TYPE = dplyr::case_when(
      grepl(
        pattern = "ISP",
        x = AGENCY,
        ignore.case = FALSE
      ) ~ "ISP",
      grepl(
        pattern = "PD|West Des Moines",
        x = AGENCY,
        ignore.case = FALSE
      ) ~ "PD",
      grepl(
        pattern = "SO|S0",
        x = AGENCY,
        ignore.case = FALSE
      ) ~ "SO",
      grepl(
        pattern = "Cons",
        x = AGENCY,
        ignore.case = FALSE
      ) ~ "Cons",
      grepl(
        pattern = "IDNR\\s(-\\s)?Parks|Iowa DNR|Iowa Parks",
        x = AGENCY,
        ignore.case = TRUE
      ) ~ "IDNR Parks",
      grepl(
        pattern = "IDOT|MVE",
        x = AGENCY,
        ignore.case = TRUE
      ) ~ "IDOT MVE",
      grepl(
        pattern = "Fair",
        x = AGENCY,
        ignore.case = TRUE
      ) ~ "Fairground LE",
      grepl(
        pattern = "Iowa Department of Public Safety",
        x = AGENCY,
        ignore.case = T
      ) ~ "IDPS",
      TRUE ~ "Unknown"
    ),
    .after = AGENCY
  ) |>
  dplyr::mutate(
    LOCATION_CITY_EVENT_CLEAN = stringr::str_squish(LOCATION_CITY_EVENT),
    LOCATION_CITY_EVENT_CLEAN = stringr::str_remove_all(
      LOCATION_CITY_EVENT_CLEAN,
      pattern = "Rural(\\s)*|Urban(\\s)*|Suburban(\\s)*|-(\\s)*|\\(|\\)"
    ),
    .after = LOCATION_CITY_EVENT
  )

# find times that are out of bounds
times_out_of_bounds <- aed_clean |>
  dplyr::filter(TIME_FROM_CALL_TO_PATIENT %in% c(-593, 605))

# find agencies that are problematic

problem_agencies <- aed_clean |>
  dplyr::filter(AGENCY_TYPE == "Unknown")

readr::write_csv(
  x = problem_agencies,
  file = file.path(output_folder, "problem_agencies.csv")
)

###_____________________________________________________________________________
# attempt a deterministic match between aed_clean and geonames location data
###_____________________________________________________________________________

# create a pattern for the words after the first name of a city
# e.g. Council (Bluffs)

# a df to observe
iowa_cities <- Iowa_Data_Final |>
  dplyr::select(name_city) |>
  dplyr::distinct() |>
  dplyr::arrange(name_city)

# a vector to create the regex
iowa_cities <- Iowa_Data_Final |>
  dplyr::select(name_city) |>
  dplyr::distinct() |>
  dplyr::arrange(name_city) |>
  dplyr::pull(name_city)

# an additional vector to help clean out county names from the location names
iowa_counties <- Iowa_Data_Final |>
  dplyr::select(name_county) |>
  dplyr::distinct() |>
  dplyr::arrange(name_county) |>
  dplyr::pull(name_county)

# the city regex
city_extension_pattern <-
  stringr::str_c(
    "\\b(?:",
    stringr::str_c(unique(iowa_cities), collapse = "|"),
    ")\\b"
  ) # - only needed to update the regex below

# Create the regex pattern
county_extension_pattern <-
  stringr::str_c(
    "\\b(?:",
    stringr::str_c(iowa_counties, collapse = "|"),
    ")\\b(?:\\sCO\\.*|\\sCOUNTY)"
  )

###_____________________________________________________________________________
# matching
# in each implementation look out for a warning from dplyr
# indicating that there are many to many relationships among x and y
# this means that we found another city that is assigned to more than 1 county
# within geonames and those are mostly errors.
# Fix this by adding to the list of filtered items for Iowa_Data_Final in the same
# format that the code is written in
###_____________________________________________________________________________

aed_final <- aed_clean |>
  dplyr::mutate(
    LOCATION_CITY_EVENT_CLEAN = stringr::str_squish(LOCATION_CITY_EVENT_CLEAN),
    LOCATION_CITY_EVENT_CLEAN = stringr::str_replace_all(
      LOCATION_CITY_EVENT_CLEAN,
      pattern = "Ft\\.",
      replacement = "Fort"
    ),
    LOCATION_CITY_EVENT_CLEAN = stringr::str_to_upper(
      LOCATION_CITY_EVENT_CLEAN
    ),
    LOCATION_CITY_EVENT_CLEAN = stringr::str_remove(
      LOCATION_CITY_EVENT_CLEAN,
      pattern = county_extension_pattern
    ),
    LOCATION_CITY_EVENT_CLEAN = dplyr::if_else(
      grepl("SGT.", x = LOCATION_CITY_EVENT_CLEAN),
      "SERGEANT BLUFF",
      dplyr::if_else(
        grepl("INDEPENCE", x = LOCATION_CITY_EVENT_CLEAN),
        "INDEPENDENCE",
        dplyr::if_else(
          grepl("AAMOSA", x = LOCATION_CITY_EVENT_CLEAN),
          "ANAMOSA",
          dplyr::if_else(
            grepl("MEDIC AMBULANCE", x = LOCATION_CITY_EVENT_CLEAN),
            NA_character_,
            dplyr::if_else(
              grepl(
                "mount air",
                x = LOCATION_CITY_EVENT_CLEAN,
                ignore.case = T
              ),
              "MOUNT AYR",
              LOCATION_CITY_EVENT_CLEAN
            )
          )
        )
      )
    ),
    LOCATION = stringr::str_extract(
      LOCATION_CITY_EVENT_CLEAN,
      pattern = city_extension_pattern
    ),
    LOCATION = dplyr::if_else(
      is.na(LOCATION) &
        LOCATION_CITY_EVENT_CLEAN != "MEDIC AMBULANCE",
      LOCATION_CITY_EVENT_CLEAN,
      LOCATION
    ),
    .after = LOCATION_CITY_EVENT_CLEAN
  ) |>
  dplyr::left_join(
    Iowa_Data_Final |>
      dplyr::select(
        name_city,
        name_county,
        `Region: Preparedness`,
        Designation,
        asciiname,
        latitude,
        longitude,
        population,
        elevation
      ),
    by = c("LOCATION" = "name_city")
  ) |>
  dplyr::relocate(
    tidyselect::all_of(
      c(
        "name_county",
        "Region: Preparedness",
        "Designation",
        "asciiname",
        "latitude",
        "longitude",
        "population",
        "elevation"
      )
    ),
    .after = LOCATION
  )

###_____________________________________________________________________________
# diagnose problems with city and county combinations below
###_____________________________________________________________________________

problems <-
  aed_final |>
  dplyr::count(UNIQUE_INCIDENT_ID, sort = T) |>
  dplyr::filter(n > 1)

###_____________________________________________________________________________
# Further EDA
###_____________________________________________________________________________

# check the distribution of the time variables
# call to patient

aed_final |>
  traumar::is_it_normal(data_name = "AED Data", x = TIME_FROM_CALL_TO_PATIENT) # fine after times were cleaned up

# call to AED on

aed_final |>
  traumar::is_it_normal(data_name = "AED Data", x = TIME_FROM_CALL_TO_AED_ON) # needs to be cleaned, based on SME input, IQR would be most realistic for outlier treatment

# time at patient to AED on

aed_final |>
  traumar::is_it_normal(data_name = "AED Data", x = TIME_FROM_PATIENT_TO_AED) # may benefit from IQR or 99% windsorizing

# total time AED used

aed_final |>
  traumar::is_it_normal(data_name = "AED Data", x = TOTAL_TIME_AED_USED) # IQR likely the best here

# time at patient to AED end

aed_final |>
  traumar::is_it_normal(data_name = "AED Data", x = TIME_AT_PATIENT_TO_END_AED) # IQR likely the best treatment method for outliers

# time of call to end AED

aed_final |>
  traumar::is_it_normal(data_name = "AED Data", x = TIME_FROM_CALL_TO_END_AED) # IQR likely the best treatment method for outliers

###_____________________________________________________________________________
# Deal with outliers
###_____________________________________________________________________________

# create a tidymodels workflow to utilize KNN imputation

# use tidymodels to create a recipe to handle missing values and outliers
# create a recipe for imputing and outlier handling ----
aed_recipe <- recipes::recipe(~., data = aed_final) |>
  recipes::step_zv(recipes::all_predictors()) |>
  recipes::step_nzv(recipes::all_predictors()) |>
  recipes::step_impute_knn(
    TIME_FROM_CALL_TO_PATIENT:TIME_FROM_CALL_TO_END_AED,
    neighbors = 5
  ) # KNN imputation

# prep and bake the recipe ----
aed_data_processed <- withr::with_seed(
  10232015, # ensures reproducible KNN imputation
  aed_recipe |>
    recipes::prep() |>
    recipes::bake(new_data = NULL) |>
    dplyr::mutate(
      dplyr::across(
        TIME_FROM_CALL_TO_PATIENT:TIME_FROM_CALL_TO_END_AED,
        ~ traumar::impute(
          x = .,
          focus = "skew",
          method = "winsorize",
          percentile = 0.95
        )
      )
    )
)

# use custom function impute() to address outliers
aed_final_impute <- aed_data_processed |>
  dplyr::mutate(
    Season = traumar::season(DATE_OF_USE),
    Day_Name = lubridate::wday(DATE_OF_USE, label = T),
    Weekday_Weekend = traumar::weekend(DATE_OF_USE),
    .after = DATE_OF_USE
  ) |>
  dplyr::mutate(
    SURVIVAL = dplyr::case_when(
      is.na(SURVIVAL) ~ RESULT_PATIENT_EXPIRED,
      TRUE ~ SURVIVAL
    )
  ) |>
  dplyr::mutate(
    Designation = dplyr::if_else(is.na(Designation), "Rural", Designation)
  ) |>
  dplyr::mutate(
    AGENCY_CLEAN = stringr::str_to_upper(stringr::str_remove_all(
      AGENCY,
      pattern = "\\s*\\(.\\)"
    )),
    .after = AGENCY
  )

# check the distribution of the time variables
# call to patient

aed_final_impute |>
  traumar::is_it_normal(data_name = "AED Data", x = TIME_FROM_CALL_TO_PATIENT) # fine after times were cleaned up

# call to AED on

aed_final_impute |>
  traumar::is_it_normal(data_name = "AED Data", x = TIME_FROM_CALL_TO_AED_ON) # needs to be cleaned, based on SME input, IQR would be most realistic for outlier treatment

# time at patient to AED on

aed_final_impute |>
  traumar::is_it_normal(data_name = "AED Data", x = TIME_FROM_PATIENT_TO_AED) # may benefit from IQR or 99% windsorizing

# total time AED used

aed_final_impute |>
  traumar::is_it_normal(data_name = "AED Data", x = TOTAL_TIME_AED_USED) # IQR likely the best here

# time at patient to AED end

aed_final_impute |>
  traumar::is_it_normal(data_name = "AED Data", x = TIME_AT_PATIENT_TO_END_AED) # IQR likely the best treatment method for outliers

# time of call to end AED

aed_final_impute |>
  traumar::is_it_normal(data_name = "AED Data", x = TIME_FROM_CALL_TO_END_AED) # IQR likely the best treatment method for outliers

# check for missingness
missing_location <- aed_final_impute |>
  dplyr::filter(dplyr::if_any(
    LOCATION_CITY_EVENT:`Region: Preparedness`,
    ~ is.na(.)
  )) |>
  dplyr::select(LOCATION_CITY_EVENT:`Region: Preparedness`) |>
  dplyr::distinct()

# check on designation

explore <- aed_final_impute |>
  dplyr::filter(is.na(Designation))

###_____________________________________________________________________________
# Export the final file to .csv for analyses in Tableau
###_____________________________________________________________________________
readr::write_csv(
  x = aed_final_impute,
  file = file.path(
    output_folder,
    stringr::str_c(
      stringr::str_c(
        "aed",
        "final",
        lubridate::year(Sys.time()),
        lubridate::month(Sys.time()),
        lubridate::day(Sys.time()),
        lubridate::hour(Sys.time()),
        lubridate::minute(Sys.time()),
        round(lubridate::second(Sys.time())),
        sep = "_"
      ),
      ".csv"
    )
  )
)

################################################################################
### End  #######################################################################
################################################################################
