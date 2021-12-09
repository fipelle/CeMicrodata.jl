# CeMicrodata.jl
```CeMicrodata.jl``` pulls data from the Consumer Expenditure (CE) Public Use Microdata (PUMD) into Julia.

```julia
import Pkg;
Pkg.add("CeMicrodata");
```

Note: the package depends on ```unzip```.

## Example

```julia
include("./src/CeMicrodata.jl"); using Main.CeMicrodata;
using Plots, StatsPlots;
prefixes=["itbi", "mtbi", "fmli"];
output = get_data(prefixes, true, 1984, 2020);
hh_output = get_hh_level(output[1], is_itbi=true, quarterly_aggregation=true);
@df hh_output violin(:REF_DATE, :HH_DATA)
```
