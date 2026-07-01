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
ems_raw = CSV.read(ems_data_path, DataFrame)

# clean names
ems_raw = @chain ems_raw begin
    @clean_names
end;

# manipulations to prepare EMS data for analysis
# clean the EMS data for comparison
ems_aed = @chain ems_raw begin
    @mutate(incident_date = TidierDates.mdy(
        TidierStrings.str_remove_all(incident_date, "12:00:00 AM")
        ),
        across(matches("date_time"), x -> TidierDates.mdy_hms(
            TidierStrings.str_remove(x, r"\\sAM$|\\sPM$")
        ))
    )
end;

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
  dplyr::filter(
    nchar(Agency_Number_d_Agency_02) == 7
  )