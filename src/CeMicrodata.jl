__precompile__()

module CeMicrodata

    # Dependencies
    using CSV, Dates, DataFrames, Downloads, Logging;

    # Custom dependencies
    local_path = dirname(@__FILE__);
    include("$(local_path)/types.jl");
    include("$(local_path)/get_data.jl");

    # Export
    export QuarterlyDataFrames, get_data;
end