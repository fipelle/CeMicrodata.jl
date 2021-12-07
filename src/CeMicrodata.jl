__precompile__()

module CeMicrodata

    # Dependencies
    using CSV, DataStructures, Dates, DataFrames, Downloads, Logging;

    # Custom dependencies
    local_path = dirname(@__FILE__);
    include("$(local_path)/get_data.jl");
    include("$(local_path)/transform_data.jl");

    # Export
    export get_data, get_hh_level_mtbi;
end