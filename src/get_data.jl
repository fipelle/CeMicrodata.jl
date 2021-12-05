"""
    download_csv_files(ref_year::String, is_interview_survey::Bool, download_folder::String)

Download csv files for a given reference year and survey (interview / diary).
"""
function download_csv_files(ref_year::String, is_interview_survey::Bool, download_folder::String)
    survey_id = ifelse(is_interview_survey, "intrvw$(ref_year[end-1:end])", "diary$(ref_year[end-1:end])");
    Downloads.download("https://www.bls.gov/cex/pumd/data/comma/$(survey_id).zip", "$(download_folder)/$(survey_id).zip");
    run(`unzip -qq $(download_folder)/$(survey_id).zip -d $(download_folder)/`);
    return survey_id;
end

"""
    csv_files_to_dataframes(survey_id::String, download_folder::String, prefixes::Vector{String})

Convert the downloaded csv files of interest (identified via the use of `prefixes`) to Julia data.
"""
function csv_files_to_dataframes(survey_id::String, download_folder::String, prefixes::Vector{String})
    
    # Memory pre-allocation: sorting problem
    last = "";
    buffer = SortedDict{String, DataFrame}();

    # Memory pre-allocation: output
    output = Vector{SortedDict{String, DataFrame}}(undef, length(prefixes));    

    # Accounts for naming inconsistencies in the folders
    survey_path = "$(download_folder)/$(survey_id)";
    readdir_output = sort(readdir(survey_path));
    if "$(survey_id)" ∈ readdir_output
        survey_path = "$(download_folder)/$(survey_id)/$(survey_id)";
        readdir_output = sort(readdir(survey_path));
    end

    # Loop over the content in `readdir_output` and focus on the csv files
    for file_name_ext in readdir_output
        file_name = split(file_name_ext, ".")[1];
        file_prefix = file_name[1:end-3];

        # Proceed if `file_prefix` is in the target prefixes
        if !isnothing(findfirst(".", file_name_ext)) && (file_prefix ∈ prefixes)

            new_SortedDict_entry = SortedDict("$(file_name)" => CSV.read("$(survey_path)/$(file_name_ext)", DataFrame));

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
    get_data(prefixes::Vector{String}, is_interview_survey::Bool, from_year::Int64, to_year::Int64)

Get data of interest from `from_year` to `to_year`.
"""
function get_data(prefixes::Vector{String}, is_interview_survey::Bool, from_year::Int64, to_year::Int64)
    
    # Memory pre-allocation: output
    n_prefixes = length(prefixes);
    output = Vector{SortedDict{String, DataFrame}}(undef, n_prefixes);    

    for t=from_year:to_year
        @info("Downloading survey referring to year $(t)");
        download_folder = mktempdir(prefix="ce_pumd_", cleanup=true);
        survey_id = download_csv_files(string(t), is_interview_survey, download_folder);
        new_entries = csv_files_to_dataframes(survey_id, download_folder, prefixes);
        for i=1:n_prefixes
            if isassigned(new_entries, i)
                if isassigned(output, i)
                    merge!(output[i], new_entries[i])
                else
                    output[i] = new_entries[i];
                end
            end
        end
    end

    return output;
end