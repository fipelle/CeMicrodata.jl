"""
    get_hh_level_mtbi(mtbi_input::Array{SortedDict{String, DataFrame}})

Return the mtbi data at household level, in a "long" DataFrame format.
"""
function get_hh_level_mtbi(mtbi_input::Array{SortedDict{String, DataFrame}})
    
    # Memory pre-allocation for output
    mtbi_output = DataFrame();

    # Loop over mtbi tables
    for entry in mtbi_input
        for (k,v) in entry

            # Copy original data
            v_copy = copy(v); # this line slows done the code, but allows to compute the hh level data without changing the input mtbi table
            transform!(v_copy, [:REF_YR, :REF_MO] => ByRow((year, month) -> Dates.lastdayofmonth(Date(year, month))) => :REF_DATE);

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
    end
    
    return mtbi_output;
end