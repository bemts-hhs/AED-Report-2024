# _____________________________________________________________________________
# Package setup and documentation
# _____________________________________________________________________________

using Pkg

# Ensure required packages are available
# Pkg.activate(".");
# Pkg.instantiate();

# only need to install packages the first time
# Pkg.add(
#     [
#     "Tidier", 
#     "TidierPlots", 
#     "TidierDates", 
#     "Dates", 
#     "DotEnv", 
#     "CSV", 
#     "XLSX", 
#     "DataFrames", 
#     "Quarto", 
#     "PrettyTables",
#     "Random"
#     ]
# );

# Load packages
using Tidier
using TidierDates
using Dates
using TidierPlots
using DotEnv
using CSV
using XLSX
using DataFrames
using Quarto
using PrettyTables
using Random

# Create .env file if it does not exist
if !isfile(".env")
	write(
		".env",
		"""
# Raw AED dataset
aed_env=

# EMS data
ems_data_env=

# Iowa county and district data
iowa_county_district_env=

# File outputs
output_directory=
""",
	)
else
	@info "File `.env` was found in the target directory."
end;

# Load .env file into ENV[]
DotEnv.config();
DotEnv.load!()

# _____________________________________________________________________________
# generate_random_id(): creates random identifiers analogous to the R function
# _____________________________________________________________________________

function generate_random_id(n::Integer; seed::Union{Int, Nothing} = 12345)
	if seed isa Int
		Random.seed!(seed)
	elseif isnothing(seed)
		@info "Random seed was not set. Results will not be reproducible."
	end

	# Character pool (upper + lower case letters)
	chars = [collect('A':'Z'); collect('a':'z'); collect('A':'Z')]

	out = Vector{String}(undef, n)

	for i in 1:n
		letters_part = String(rand(chars, 10))
		numbers_part = rand(1_000_000_000:9_999_999_999)
		out[i] = string(letters_part, "-", numbers_part)
	end

	return out
end;
