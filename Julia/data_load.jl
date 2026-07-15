###_____________________________________________________________________________
# Prepare Data for Analysis
# Run this script first before moving on to analyses
###_____________________________________________________________________________

# Get environment variables into the global environment
aed_data_path = ENV["aed_env"];
ems_data_path = ENV["ems_data_env"];
iowa_county_district_path = ENV["iowa_county_district_env"];
output_folder = ENV["output_directory"];

###_____________________________________________________________________________
# read in the data
###_____________________________________________________________________________

# read in the data
ems_raw = CSV.read(ems_data_path, DataFrame);

# clean names
ems_raw = @chain ems_raw begin
    @clean_names
    @rename_with str -> str_remove_all(str, r"\(|\)")
    @rename_with str -> str_replace_all(str, r"=", "_")
end;

# manipulations to prepare EMS data for analysis
# clean the EMS data for comparison
ems_aed = @chain ems_raw begin
    @mutate(
    incident_date = TidierDates.mdy.(incident_date),
    incident_date_time =
        TidierDates.mdy_hms.(incident_date_time),

    incident_unit_notified_by_dispatch_date_time_e_times_03 =
        TidierDates.mdy_hms.(incident_unit_notified_by_dispatch_date_time_e_times_03),

    incident_unit_arrived_on_scene_date_time_e_times_06 =
        TidierDates.mdy_hms.(incident_unit_arrived_on_scene_date_time_e_times_06),

    incident_unit_arrived_at_patient_date_time_e_times_07 =
        TidierDates.mdy_hms.(incident_unit_arrived_at_patient_date_time_e_times_07),

    procedure_performed_date_time_e_procedures_01 =
        TidierDates.mdy_hms.(procedure_performed_date_time_e_procedures_01)
    )
    @mutate(

        incident_dispatch_notified_to_unit_arrived_on_scene_in_minutes = 
        difftime(incident_unit_arrived_on_scene_date_time_e_times_06, incident_unit_notified_by_dispatch_date_time_e_times_03, "minutes"
        ),
      incident_dispatch_notified_to_unit_arrived_at_patient_in_minutes = 
      TidierDates.difftime.(
        incident_unit_arrived_at_patient_date_time_e_times_07,
        incident_unit_notified_by_dispatch_date_time_e_times_03,
        "minutes"
      ),
      cardiac_arrest_who_provided_cpr_prior_to_ems_arrival_list_3_4_e_arrest_06_3_5_it_arrest_106 = TidierStrings.str_remove_all.(
      cardiac_arrest_who_provided_cpr_prior_to_ems_arrival_list_3_4_e_arrest_06_3_5_it_arrest_106, "\""
    )
)
  @filter length.(string.(agency_number_d_agency_02)) .== 7
end;

###_____________________________________________________________________________
# Analyze EMS data
###_____________________________________________________________________________

# get all unique procedures, examine
all_procedures = @chain ems_aed begin
  @select procedure_performed_description_e_procedures_03
  @distinct
  @arrange procedure_performed_description_e_procedures_03
end;

#= 
get patients with defibrillation procedures
select only procedure, date/time, and # attempts
get the unique run IDs with defib using filter
get the @distinct rows which will be unique run ID, procedure, date/time, and # of shocks, so you have each row as a @distinct procedure with date/time and # shocks
take the sum of the procedures per Run ID which gives you # of shocks total
over all defib procedures  
=#

# 1. Select relevant variables
df = select(
    ems_aed,
    :fact_incident_pk,
    :procedure_performed_description_e_procedures_03,
    :procedure_performed_date_time_e_procedures_01,
    :procedure_number_of_attempts_e_procedures_05,
)

# 2. Filter defibrillation procedures
pattern = r"automatic external cardiac defibrillator (physical object)|cv - automated external defibrillator|automatic cardiac defibrillator (physical object)|cv - defibrillation - manual|defibrillation, aed|electrical cardioversion (& defibrillation)|cv - cardioversion|cardiac resuscitation|management of external defibrillation"i

df = df[
    occursin.(pattern, coalesce.(df.procedure_performed_description_e_procedures_03, "")),
    :
]

# 3. Distinct rows
df = @distinct df

# 4. Coalesce missings
df.procedure_performed_description_e_procedures_03 =
    coalesce.(df.procedure_performed_description_e_procedures_03, "")

df.procedure_performed_date_time_e_procedures_01 =
    coalesce.(df.procedure_performed_date_time_e_procedures_01, "")

df.procedure_number_of_attempts_e_procedures_05 =
    coalesce.(df.procedure_number_of_attempts_e_procedures_05, 1)

# 5. Group by run ID
gdf = groupby(df, :fact_incident_pk)

# 6. Summarize exactly as your mutate block intended
ems_aed_defib = combine(gdf,
    :procedure_number_of_attempts_e_procedures_05 => sum => :shocks,
    :procedure_performed_description_e_procedures_03 =>
        (x -> join(x, ", ")) => :procedure_performed_description_e_procedures_03,
    :procedure_performed_date_time_e_procedures_01 =>
        (x -> join(x, ", ")) => :procedure_performed_date_time_e_procedures_01,
    :procedure_number_of_attempts_e_procedures_05 =>
        (x -> join(string.(x), ", ")) => :procedure_number_of_attempts_e_procedures_05,
)

# 7. Final distinct
ems_aed_defib = unique(ems_aed_defib)

# just get the shocks as a separate object for the join
ems_shocks = select(
 ems_aed_defib, 
 :fact_incident_pk,
 :shocks
)

# read in location data for regions / urbanicity
location <- readxl::read_excel(
  iowa_county_district_path
)

# deal with multiple procedures to reduce the rows to 1 row = 1 run, no duplication
ems_aed_runs <- ems_aed |>
  @mutate(
    incident_day = lubridate::wday(incident_date, label = t, abbr = f),
    weekday_weekend = traumar::weekend(incident_date),
    season = traumar::season(incident_date),
    .after = incident_date_time
  ) |>
  @group_by(fact_incident_pk) |>
  @mutate(@across(
    c(
      incident_complaint_reported_by_dispatch_dispatch_reason_e_dispatch_01,
      situation_provider_primary_impression_description_only_e_situation_11,
      procedure_performed_description_e_procedures_03,
      procedure_performed_date_time_e_procedures_01,
      medication_given_or_administered_description_e_medications_03,
      medication_response_e_medications_07
    ),
    ~ stringr::str_c(unique(.), collapse = ", ")
  )) |>
  @mutate(
    procedure_number_of_attempts_e_procedures_05 = stringr::str_c(
      procedure_number_of_attempts_e_procedures_05,
      collapse = ", "
    )
  ) |>
  @mutate(
    witnessed = !grepl(
      pattern = "not",
      x = cardiac_arrest_witnessed_by_list_e_arrest_04,
      ignore.case = t
    ),
    .after = cardiac_arrest_witnessed_by_list_e_arrest_04
  ) |>
  naniar::replace_with_na(
    replace = list(
      cardiac_arrest_cpr_provided_prior_to_ems_arrival_3_4_e_arrest_05_3_5_it_arrest_105 = c(
        "not recorded",
        "not applicable"
      )
    )
  ) |>
  @mutate(
    cardiac_arrest_cpr_provided_prior_to_ems_arrival_3_4_e_arrest_05_3_5_it_arrest_105 = @if_else(
      is.na(
        cardiac_arrest_cpr_provided_prior_to_ems_arrival_3_4_e_arrest_05_3_5_it_arrest_105
      ) &
        (!is.na(
          cardiac_arrest_who_initiated_cpr_3_4_it_arrest_008_3_5_e_arrest_20
        ) &
          !cardiac_arrest_who_initiated_cpr_3_4_it_arrest_008_3_5_e_arrest_20 %in%
            c("responding ems personnel", "first responder (ems)")) |
        !is.na(
          cardiac_arrest_who_provided_cpr_prior_to_ems_arrival_list_3_4_e_arrest_06_3_5_it_arrest_106
        ),
      "yes",
      cardiac_arrest_cpr_provided_prior_to_ems_arrival_3_4_e_arrest_05_3_5_it_arrest_105
    ),
    cardiac_arrest_cpr_provided_prior_to_ems_arrival_3_4_e_arrest_05_3_5_it_arrest_105 = tidyr::replace_na(
      cardiac_arrest_cpr_provided_prior_to_ems_arrival_3_4_e_arrest_05_3_5_it_arrest_105,
      replace = "no"
    ),
    .after = cardiac_arrest_cpr_provided_prior_to_ems_arrival_3_4_e_arrest_05_3_5_it_arrest_105
  ) |>
  @ungroup() |>
  @full_join(ems_shocks, by = @join_by(fact_incident_pk)) |>
  @relocate(
    shocks,
    .after = procedure_number_of_attempts_e_procedures_05
  ) |>
  @distinct(fact_incident_pk, .keep_all = t) |>
  tidyr::replace_na(list(situation_possible_overdose = false)) |>
  @mutate(
    @across(
      c(
        incident_dispatch_notified_to_unit_arrived_at_patient_in_minutes,
        incident_dispatch_notified_to_unit_arrived_on_scene_in_minutes
      ),
      ~ traumar::impute(., focus = "missing", method = "median")
    ),
    @across(
      c(
        incident_dispatch_notified_to_unit_arrived_at_patient_in_minutes,
        incident_dispatch_notified_to_unit_arrived_on_scene_in_minutes
      ),
      ~ traumar::impute(
        .,
        focus = "skew",
        method = "winsorize",
        percentile = 0.95
      )
    )
  ) |>
  @mutate(
    cardiac_arrest_patient_outcome_at_end_of_ems_event_e_arrest_18 = @if_else(
      is.na(
        cardiac_arrest_patient_outcome_at_end_of_ems_event_e_arrest_18
      ) |
        cardiac_arrest_patient_outcome_at_end_of_ems_event_e_arrest_18 %in%
          c("not applicable", "not recorded") &
          grepl(
            pattern = "dead",
            x = disposition_incident_patient_disposition_3_4_e_disposition_12_3_5_it_disposition_112,
            ignore.case = t
          ),
      "expired in the field",
      cardiac_arrest_patient_outcome_at_end_of_ems_event_e_arrest_18
    )
  ) |>
  @mutate(
    rosc_ed = @case_when(
      grepl(
        pattern = "rosc in the ed",
        x = cardiac_arrest_patient_outcome_at_end_of_ems_event_e_arrest_18,
        ignore.case = t
      ) ~ true,
      true ~ false
    ),
    rosc_field = @case_when(
      grepl(
        pattern = "rosc in the field",
        x = cardiac_arrest_patient_outcome_at_end_of_ems_event_e_arrest_18,
        ignore.case = t
      ) ~ true,
      true ~ false
    ),
    ongoing_resus_ems = @case_when(
      grepl(
        pattern = "ongoing resuscitation by other ems",
        x = cardiac_arrest_patient_outcome_at_end_of_ems_event_e_arrest_18,
        ignore.case = t
      ) ~ true,
      true ~ false
    ),
    ongoing_resus_ed = @case_when(
      grepl(
        pattern = "ongoing resuscitation in ed",
        x = cardiac_arrest_patient_outcome_at_end_of_ems_event_e_arrest_18,
        ignore.case = t
      ) ~ true,
      true ~ false
    ),
    expire_field = @case_when(
      grepl(
        pattern = "expired in the field",
        x = cardiac_arrest_patient_outcome_at_end_of_ems_event_e_arrest_18,
        ignore.case = t
      ) ~ true,
      true ~ false
    ),
    expire_ed = @case_when(
      grepl(
        pattern = "expired in the ed",
        x = cardiac_arrest_patient_outcome_at_end_of_ems_event_e_arrest_18,
        ignore.case = t
      ) ~ true,
      true ~ false
    ),
    survival = @case_when(
      grepl(
        pattern = "expire",
        x = cardiac_arrest_patient_outcome_at_end_of_ems_event_e_arrest_18,
        ignore.case = t
      ) ~ false,
      true ~ true
    ),
    .after = cardiac_arrest_patient_outcome_at_end_of_ems_event_e_arrest_18
  ) |>
  @left_join(
    location,
    by = c("scene_incident_county_name_e_scene_21" = "county")
  )

