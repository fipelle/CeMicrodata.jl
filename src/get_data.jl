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
    
    # Memory pre-allocation
    last = "";
    buffer = Vector{DataFrame}();
    output = Vector{QuarterlyDataFrames}(undef, length(prefixes));    

    # Loop over downloaded csv files
    for file_name_ext in sort(readdir("$(download_folder)/$(survey_id)"))
        file_name = split(file_name_ext, ".")[1];

        # Proceed if `file_name[1:end-3]` is in the target prefixes
        if file_name[1:end-3] ∈ prefixes

            # Populate `buffer`
            if file_name == last
                push!(buffer, CSV.read("$(download_folder)/$(survey_id)/file_name_ext", DataFrame));
            
            # New iteration
            else
                # Populate `output`
                if length(buffer) > 0
                    coord_current_file = findfirst(file_name[1:end-3] .∈ prefixes);
                    output[coord_current_file] = QuarterlyDataFrames(buffer...);
                end

                # Re-initialise
                empty!(buffer);
                push!(buffer, CSV.read("$(download_folder)/$(survey_id)/file_name_ext", DataFrame));
                last = copy(file_name);
            end
        end
    end
end