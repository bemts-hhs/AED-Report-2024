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

ems_raw = CSV.read(ems_data_path, DataFrame)