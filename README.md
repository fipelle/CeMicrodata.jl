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
prefixes=["itbi", "memi"];
output = get_data(prefixes, true, 2019, 2020);
```

```julia
output[1]
```
