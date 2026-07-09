###_____________________________________________________________________________
# Prepare Data for Analysis
# Run this script first before moving on to analyses
###_____________________________________________________________________________

# Get environment variables into the global environment
aed_data_path = ENV["aed_env"]
ems_data_path = ENV["ems_data_env"]
iowa_county_district_path = ENV["iowa_county_district_env"]
output_folder = ENV["output_directory"]

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
    @mutate(incident_date = TidierDates.mdy(
        TidierStrings.str_remove_all.(incident_date, "12:00:00 AM")
        ),
        across(matches("date_time"), str -> TidierStrings.str_remove.(str, r"\\s(a|p)m"i)
        )#,
  #       incident_dispatch_notified_to_unit_arrived_on_scene_in_minutes = TidierDates.difftime.(
  #       `incident_unit_arrived_on_scene_date_time_(e_times_06)`,
  #       `incident_unit_notified_by_dispatch_date_time_(e_times_03)`,
  #       "minutes"
  #     ),
  #     incident_dispatch_notified_to_unit_arrived_at_patient_in_minutes = 
  #     TidierDates.difftime.(
  #       `incident_unit_arrived_at_patient_date_time_(e_times_07)`,
  #       `incident_unit_notified_by_dispatch_date_time_(e_times_03)`,
  #       "minutes"
  #     ),
  #     `cardiac_arrest_who_provided_cpr_prior_to_ems_arrival_list_(3_4=e_arrest_06_3_5=it_arrest_106)` = TidierStrings.str_remove_all.(
  #     `cardiac_arrest_who_provided_cpr_prior_to_ems_arrival_list_(3_4=e_arrest_06_3_5=it_arrest_106)`, "\""
  #   )
  )
  # @filter length.(`agency_number_(d_agency_02)`) .== 7
end;