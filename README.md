# CeMicrodata.jl
```CeMicrodata.jl``` pulls data from the Consumer Expenditure (CE) Public Use Microdata (PUMD) into Julia.

## Installation

The package can be installed with the Julia package manager.
From the Julia REPL, type `]` to enter the Pkg REPL mode and run:

```
pkg> add CeMicrodata
```

Or, equivalently, via the `Pkg` API:

```julia
julia> import Pkg; Pkg.add("CeMicrodata")
```

Note: the package depends on ```unzip``` which needs to be manually installed and available from the terminal.

## Example

```julia
# Collect income data
using CeMicrodata, CSV, Dates, DataFrames, PlotlyJS, Statistics;
prefixes=["itbi", "mtbi", "fmli"];
output = get_data(prefixes, true, 1990, 2020);
hh_output = get_hh_level(output[1], is_itbi=true, UCC_selection=["900000"], quarterly_aggregation=false);

# Aggregate data
hh_output_aggregate = combine(groupby(hh_output, [:REF_DATE]), :HH_DATA=>mean);
PlotlyJS.plot(
    hh_output_aggregate,
    x=:REF_DATE, 
    y=:HH_DATA_mean, 
)

# Plotting
hh_output_plot = copy(hh_output);
hh_output_plot[!,:REF_YEAR] = lastdayofyear.(hh_output_plot[!,:REF_DATE]);
hh_output_plot = combine(groupby(hh_output_plot, [:CUSTOM_CUID, :REF_YEAR]), :HH_DATA=>sum);
PlotlyJS.plot(
    hh_output_plot,
    x=:REF_YEAR, 
    y=:HH_DATA_sum, 
    kind="violin", 
    meanline_visible=true, 
    side="positive", 
    points=false
)
```