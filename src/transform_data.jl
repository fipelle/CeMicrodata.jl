"""
    get_hh_level_mtbi(mtbi_input::SortedDict{String, DataFrame}; quarterly_aggregation::Bool=false)

Return the mtbi data at household level, in a "long" DataFrame format.
"""
function get_hh_level_mtbi(mtbi_input::SortedDict{String, DataFrame}; quarterly_aggregation::Bool=false)
    
    # Memory pre-allocation for output
    mtbi_output = DataFrame();

    # Loop over mtbi tables
    for (k,v) in mtbi_input

        # Copy original data
        v_copy = copy(v); # this line slows done the code, but allows to compute the hh level data without changing the input mtbi table
        for row in eachrow(v_copy)
            if row[:REF_YR] < 20 # YY rather than YYYY and referring to 20YY
                row[:REF_YR] += 2000;
            elseif 20 < row[:REF_YR] < 100 # YY rather than YYYY and referring to 19YY
                row[:REF_YR] += 1900;
            end
        end

        if quarterly_aggregation == false
            transform!(v_copy, [:REF_YR, :REF_MO] => ByRow((year, month) -> Dates.lastdayofmonth(Date(year, month))) => :REF_DATE);
        else
            transform!(v_copy, [:REF_YR, :REF_MO] => ByRow((year, month) -> Dates.lastdayofquarter(Date(year, month))) => :REF_DATE);
        end

        # Construct grouped data
        v_grouped = combine(groupby(v_copy, [:NEWID, :REF_DATE]), :COST=>sum);
        rename!(v_grouped, Dict(:COST_sum => "EXPEND"));

        # Update output
        if size(mtbi_output, 1) == 0
            mtbi_output = copy(v_grouped);
        else
            append!(mtbi_output, v_grouped);
        end
    end
    
    return mtbi_output;
end