"""
    get_hh_level(monthly_input::SortedDict{String, DataFrame}; is_itbi::Bool=false, is_mtbi::Bool=false, quarterly_aggregation::Bool=false)

Convert itbi and mtbi data at household level, in a "long" DataFrame format.
"""
function get_hh_level(monthly_input::SortedDict{String, DataFrame}; is_itbi::Bool=false, is_mtbi::Bool=false, quarterly_aggregation::Bool=false)
    
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
    for (k,v) in monthly_input

        # Copy original data
        v_copy = copy(v); # this line slows done the code, but allows to compute the hh level data without changing the input monthly table
        for row in eachrow(v_copy)
            if row[ref_year] < 20 # YY rather than YYYY and referring to 20YY
                row[ref_year] += 2000;
            elseif 20 < row[ref_year] < 100 # YY rather than YYYY and referring to 19YY
                row[ref_year] += 1900;
            end
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

        elseif is_mtbi
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