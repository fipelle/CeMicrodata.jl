"""
    QuarterlyDataFrames(...)

Convenient structure to handle the calendar quarters in which the interview occurred / booklet was placed.
"""
struct QuarterlyDataFrames
    q1::Union{DataFrame, Nothing}
    q2::Union{DataFrame, Nothing}
    q3::Union{DataFrame, Nothing}
    q4::Union{DataFrame, Nothing}
end

# Constructor for Interview data releases from 2020 onwards.
QuarterlyDataFrames(q2::DataFrame, q3::DataFrame, q4::DataFrame) = QuarterlyDataFrames(nothing, q2, q3, q4);