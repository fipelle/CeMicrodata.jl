"""
    download_ce_pumd_files(ref_year::String, is_interview_survey::Bool, download_prefix::String, download_folder::String)

Download ce_pumd files for a given reference year and survey (interview / diary).
"""
function download_ce_pumd_files(ref_year::String, is_interview_survey::Bool, download_prefix::String, download_folder::String)
    
    # Setup headers
    headers = Dict(
        "User-Agent" => "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept" => "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language" => "en-GB,en;q=0.5",
        "Accept-Encoding" => "gzip, deflate, br",
        "Connection" => "keep-alive",
        "Upgrade-Insecure-Requests" => "1",
        "Sec-Fetch-Dest:" => "document",
        "Sec-Fetch-Mode:" => "navigate",
        "Sec-Fetch-Site:" => "none",
        "Sec-Fetch-User:" => "?1"
    )
  
    # Download ce_pumd file
    survey_id = ifelse(is_interview_survey, "intrvw$(ref_year[end-1:end])", "diary$(ref_year[end-1:end])");
    Downloads.download("https://www.bls.gov/cex/pumd/data/$(download_prefix)/$(survey_id).zip", "$(download_folder)/$(survey_id).zip", headers=headers);
    run(`unzip -qq $(download_folder)/$(survey_id).zip -d $(download_folder)/$(survey_id)`);
    return survey_id;
end

"""
    safe_parse_id(id)

Safely parse values to Int64.
"""
safe_parse_id(id::Number) = parse(Int64, string(Int64(id))[1:end-1]);

function safe_parse_id(id::String15)
    if id[end] != "."
        return parse(Int64, id[1:end-1]);
    else
        return parse(Int64, "$(id[1:end-2]).0");
    end
end

"""
    harmonize_column_types(data_dict::SortedDict{String, DataFrame})

Harmonise column types within SortedDict of DataFrames.
"""
function harmonize_column_types(data_dict::SortedDict{String, DataFrame})
    
    # Define target types
    target_types = Union{Int64, Float64, String};
    
    for (key, df) in data_dict

        for col in names(df)

            # Check the current type of the column
            col_type = eltype(df[!, col]);
            
            if col_type <: Union{AbstractFloat, AbstractString}
                try
                    df[!, col] = convert(Vector{Int64}, df[!, col]);
                catch
                    try
                        df[!, col] = convert(Vector{Float64}, df[!, col])
                    catch
                        df[!, col] = convert(Vector{String}, df[!, col])
                    end
                end
            
            elseif col_type <: Union{Union{Missing, AbstractFloat}, Union{Missing, AbstractString}}
                try
                    df[!, col] = convert(Vector{Union{Missing, Int64}}, df[!, col]);
                catch
                    try
                        df[!, col] = convert(Vector{Union{Missing, Float64}}, df[!, col]);
                    catch
                        df[!, col] = convert(Vector{Union{Missing, String}}, df[!, col]);
                    end
                end            
            end
        end
    end
    
    return data_dict;
end

"""
    ce_pumd_files_to_dataframes(survey_id::String, download_folder::String, prefixes::Vector{String})

Convert the downloaded ce_pumd files of interest (identified via the use of `prefixes`) to Julia data.
"""
function ce_pumd_files_to_dataframes(survey_id::String, download_folder::String, prefixes::Vector{String})
    
    # Memory pre-allocation: sorting problem
    last = "";
    buffer = SortedDict{String, DataFrame}();

    # Memory pre-allocation: output
    output = Vector{SortedDict{String, DataFrame}}(undef, length(prefixes));    

    # Accounts for naming inconsistencies in the folders
    survey_path = "$(download_folder)/$(survey_id)";
    readdir_output = sort(readdir(survey_path));

    # If binary data is zipped within the original zip
    if "$(survey_id).zip" ∈ readdir_output
        run(`unzip -qq $(survey_path)/$(survey_id).zip -d $(survey_path)/`);
    end
    
    # Folder inconsistency
    if "$(survey_id)" ∈ readdir_output
        survey_path = "$(download_folder)/$(survey_id)/$(survey_id)";
        readdir_output = sort(readdir(survey_path));
    end

    # Loop over the content in `readdir_output` and focus on the ce_pumd files
    for file_name_ext in readdir_output
        file_name = split(file_name_ext, ".")[1];
        file_prefix = file_name[1:end-3];

        # Proceed if `file_prefix` is in the target prefixes
        if !isnothing(findfirst(".", file_name_ext)) && (file_prefix ∈ prefixes) # this implicitly skips the tables ending with 'x'

            if file_name[end-2] == '9'
                new_key = "$(file_prefix)_19$(file_name[end-2:end])";
            else
                new_key = "$(file_prefix)_20$(file_name[end-2:end])";
            end

            # Store current ce_pumd file into a DataFrame

            # Remove comment for csv files
            # new_SortedDict_item = CSV.read("$(survey_path)/$(file_name_ext)", missingstring=["", "."], DataFrame);
            new_SortedDict_item = DataFrame(readstat("$(survey_path)/$(file_name_ext)"));
            
            # Capitalize all column names (this is to align sas and stata files to csv)
            rename!(new_SortedDict_item, Symbol.(uppercase.(String.(names(new_SortedDict_item)))));
            
            # Include custom identifier for CUs
            if "NEWID" ∈ names(new_SortedDict_item)
                new_SortedDict_item[!, :CUSTOM_CUID] = [safe_parse_id(id) for id in new_SortedDict_item[!, :NEWID]];
            end
            
            # Generate `new_SortedDict_entry`
            new_SortedDict_entry = SortedDict(new_key => new_SortedDict_item);

            # Populate `buffer`
            if file_prefix == last
                merge!(buffer, new_SortedDict_entry);
            
            # New iteration
            else
                # Populate `output`
                if length(buffer) > 0
                    coord_current_file = findfirst(last .== prefixes);
                    output[coord_current_file] = copy(buffer);
                    empty!(buffer);
                end

                # Re-initialise
                merge!(buffer, new_SortedDict_entry);
                last = String(file_prefix);
            end
        end
    end

    if length(buffer) > 0
        coord_current_file = findfirst(last .== prefixes);
        output[coord_current_file] = copy(buffer);
    end

    return output;
end

"""
    get_data(prefixes::Vector{String}, is_interview_survey::Bool, from_year::Int64, to_year::Int64; verbose::Bool=true)

Get data of interest from `from_year` to `to_year`.
"""
function get_data(prefixes::Vector{String}, is_interview_survey::Bool, from_year::Int64, to_year::Int64; verbose::Bool=true)
    
    # Memory pre-allocation: output
    n_prefixes = length(prefixes);
    output = Vector{SortedDict{String, DataFrame}}(undef, n_prefixes);    

    for t=from_year:to_year
        if verbose
            @info("Downloading survey referring to year $(t)");
        end
        
        #=
        Remove comment to download csv files

        if t >= 2022
            download_prefix = "csv";
        else
            download_prefix = "comma";
        end
        =#

        download_prefix = "stata";
        
        download_folder = mktempdir(prefix="ce_pumd_", cleanup=true);
        survey_id = download_ce_pumd_files(string(t), is_interview_survey, download_prefix, download_folder);
        new_entries = ce_pumd_files_to_dataframes(survey_id, download_folder, prefixes);
        for i=1:n_prefixes
            if isassigned(new_entries, i)
                if isassigned(output, i)
                    merge!(output[i], harmonize_column_types(new_entries[i]))
                else
                    output[i] = harmonize_column_types(new_entries[i]);
                end
            end
        end
    end

    return output;
end

"""
    get_stubs()

Return stubs tables in DataFrame format.
"""
function get_stubs()

    # Setup headers
    headers = Dict(
        "User-Agent" => "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.0.0 Safari/537.36",
        "Accept" => "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language" => "en-GB,en;q=0.5",
        "Accept-Encoding" => "gzip, deflate, br",
        "Connection" => "keep-alive",
        "Upgrade-Insecure-Requests" => "1",
        "Sec-Fetch-Dest:" => "document",
        "Sec-Fetch-Mode:" => "navigate",
        "Sec-Fetch-Site:" => "none",
        "Sec-Fetch-User:" => "?1"
    )
  
    # Download stubs
    download_folder = mktempdir(prefix="ce_pumd_", cleanup=true);
    Downloads.download("https://www.bls.gov/cex/pumd/stubs.zip", "$(download_folder)/stubs.zip", headers=headers);
    run(`unzip -qq $(download_folder)/stubs.zip -d $(download_folder)/`);

    # Memory pre-allocation for output
    output = Array{DataFrame}(undef, 3); 

    for file_name_ext in sort(readdir("$(download_folder)/stubs/"))
        readdlm_output = readdlm("$(download_folder)/stubs/$(file_name_ext)", '\n');
        for line in readdlm_output
            if (line[1] != '*') && (line[1] != '2') # skip comments and secondary details on the UCC description
                
                # Reference year
                ref_year = file_name_ext[end-7:end-4];
                txt_offset = ifelse(parse(Int64, ref_year) < 2013, 0, 3);

                # Sort content
                content = [ref_year,                              # Reference year
                           line[1:3],                             # Type of information in the line
                           line[4:6],                             # Level of aggregation
                           line[7:69],                            # Name of the UCC
                           line[70:79+txt_offset],                # UCC lists the identifier of the UCC
                           line[80+txt_offset:82+txt_offset],     # Source or purpose of the UCC
                           line[83+txt_offset:85+txt_offset],     # Factor by which the mean has to be multiplied to match the annualized data in the published tables
                           line[86+txt_offset:end]];              # Data sections
                
                for i in 1:8
                    content[i] = strip(content[i]);
                end

                df_row = DataFrame(permutedims(content), [:ref_year, :type, :level, :name, :UCC, :source, :factor, :section]);

                # Store to the appropriate DataFrame
                survey_type = split(file_name_ext, '-')[end-1];
                if survey_type == "Diary"
                    output_coord = 1;
                elseif survey_type == "Integ"
                    output_coord = 2;
                elseif survey_type == "Inter"
                    output_coord = 3;
                end

                if isassigned(output, output_coord)
                    append!(output[output_coord], df_row);
                else
                    output[output_coord] = df_row;
                end
            end
        end
    end

    return output;
end