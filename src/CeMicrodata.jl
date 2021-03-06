__precompile__()

module CeMicrodata

    # Dependencies
    using CSV, DataFrames, DataFramesMeta, DataStructures, Dates, DelimitedFiles, Downloads, Logging, Statistics;

    # Custom dependencies
    local_path = dirname(@__FILE__);
    include("$(local_path)/get_data.jl");
    include("$(local_path)/transform_data.jl");

    # Export
    export get_data, get_stubs,
           get_hh_level, merge_fmli_files, quarterly_hh_level!;
end