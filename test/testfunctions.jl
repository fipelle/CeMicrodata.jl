include("../src/CeMicrodata.jl"); using Main.CeMicrodata;
using CSV, Test, Dates, DataFrames, PlotlyJS, Statistics;

function data_test()

	# Load expenditures on "new cars" in 2016 as test example
	prefixes=["mtbi"];
	output = get_data(prefixes, true, 2016, 2016);
	hh_output = get_hh_level(output[1], is_mtbi=true, UCC_selection=["450110"], quarterly_aggregation=false);

	# Load test data from file
	data_test_df = DataFrame(CSV.File("mtbi_test.csv"))

	# Check that the loaded data is the same as test data
	@test hh_output == data_test_df
end

function aggregation_test()

	# Load expenditures on "new cars" in 2016 as test example
	prefixes=["mtbi"];
	output = get_data(prefixes, true, 2016, 2016);
	hh_output = get_hh_level(output[1], is_mtbi=true, UCC_selection=["450110"], quarterly_aggregation=false);

	# Aggregate
	output_aggregate = combine(groupby(hh_output, [:REF_DATE]), :HH_DATA=>mean);
	
	# Load test data from file
	aggregation_test_df = DataFrame(CSV.File("aggregation_test.csv"))

	# Check that the loaded data is the same as test data
	@test output_aggregate == aggregation_test_df

end

