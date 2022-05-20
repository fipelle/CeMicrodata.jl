include("testfunctions.jl")

@testset "Tests on APIs" begin
    data_test();
end

@testset "Tests on aggregation functions" begin
    aggregation_test();
end
