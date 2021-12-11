"""
    UCC_column_as_strings!(df::DataFrame, UCCs::Vector{String})

Return `nothing`.

    UCC_column_as_strings!(df::DataFrame, UCCs::Vector{Int64})

Parse UCC column into strings.
"""
UCC_column_as_strings!(df::DataFrame, UCCs::Vector{String}) = nothing;

function UCC_column_as_strings!(df::DataFrame, UCCs::Vector{Int64})
    df[!,:UCC] = string.(df[!, :UCC]);
end

"""
    merge_fmli_files(fmli_files::SortedDict{String, DataFrame}, mnemonics::Vector{Symbol})

Merge FMLI vintages.
"""
function merge_fmli_files(fmli_files::SortedDict{String, DataFrame}, mnemonics::Vector{String})

    # Convenient conversion
    mnemonics_sym = Symbol.(unique(vcat("QINTRVYR", "QINTRVMO", mnemonics)));
    
    # Memory pre-allocation for output
    output = DataFrame();

    # Loop over monthly tables
    for (k, v) in fmli_files

        # Select target variables
        v_selection = copy(v[!, mnemonics_sym]);
        for row in eachrow(v_selection)
            if row[:QINTRVYR] < 20 # YY rather than YYYY and referring to 20YY
                row[:QINTRVYR] += 2000;
            elseif 20 < row[:QINTRVYR] < 100 # YY rather than YYYY and referring to 19YY
                row[:QINTRVYR] += 1900;
            end
        end

        # Add REF_DATE
        transform!(v_selection, [:QINTRVYR, :QINTRVMO] => ByRow((year, month) -> Dates.lastdayofmonth(Date(year, month))) => :REF_DATE);

        # Update output
        if size(output, 1) == 0
            output = v_selection;
        else
            append!(output, v_selection);
        end
    end

    return output;
end

"""
    get_hh_level(input_dict::SortedDict{String, DataFrame}; is_itbi::Bool=false, is_mtbi::Bool=false, UCC_selection::Union{Nothing, Vector{String}}=nothing, quarterly_aggregation::Bool=false)

Convert income or expenditure data at household level (in a "long" DataFrame format)
"""
function get_hh_level(input_dict::SortedDict{String, DataFrame}; is_itbi::Bool=false, is_mtbi::Bool=false, UCC_selection::Union{Nothing, Vector{String}}=nothing, quarterly_aggregation::Bool=false)
    
    if (is_itbi && is_mtbi) || (!is_itbi && !is_mtbi)
        error("`is_itbi` or `is_mtbi` must be true.");
    end

    if is_itbi
        ref_year = :REFYR;
        ref_month = :REFMO;
    else
        ref_year = :REF_YR;
        ref_month = :REF_MO;
    end

    # Memory pre-allocation for output
    output = DataFrame();

    # Loop over monthly tables
    for (k,v) in input_dict

        # Copy original data
        v_copy = copy(v); # this line slows done the code, but allows to compute the hh level data without changing the input monthly table
        UCC_column_as_strings!(v_copy, v_copy[!,:UCC]);

        for row in eachrow(v_copy)
            if row[ref_year] < 20 # YY rather than YYYY and referring to 20YY
                row[ref_year] += 2000;
            elseif 20 < row[ref_year] < 100 # YY rather than YYYY and referring to 19YY
                row[ref_year] += 1900;
            end
        end

        if !isnothing(UCC_selection)
            transform!(v_copy, :UCC => ByRow(ucc -> ucc ∈ UCC_selection) => :include_UCC);
            v_copy = v_copy[findall(v_copy[!,:include_UCC]), :];
        end

        if quarterly_aggregation == false
            transform!(v_copy, [ref_year, ref_month] => ByRow((year, month) -> Dates.lastdayofmonth(Date(year, month))) => :REF_DATE);
        else
            transform!(v_copy, [ref_year, ref_month] => ByRow((year, month) -> Dates.lastdayofquarter(Date(year, month))) => :REF_DATE);
        end

        # Construct grouped data
        if is_itbi
            v_grouped = combine(groupby(v_copy, [:NEWID, :REF_DATE]), :VALUE=>sum);
            rename!(v_grouped, Dict(:VALUE_sum => "HH_DATA"));

        else
            v_grouped = combine(groupby(v_copy, [:NEWID, :REF_DATE]), :COST=>sum);
            rename!(v_grouped, Dict(:COST_sum => "HH_DATA"));
        end

        # Update output
        if size(output, 1) == 0
            output = copy(v_grouped);
        else
            append!(output, v_grouped);
        end
    end
    
    return output;
end