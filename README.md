# README: Jakob Kuemmerle, Music Data Processing Pipeline

### Overview
This repository contains a Python implementation of a data processing pipeline for analyzing music data. The pipeline consists of several stages including splitting, mapping, shuffling, and reducing.

The data originates from the following source:
The following repository explains how to access the data:

#### Components
1. **split2.py**: Splits a CSV file into multiple chunks to facilitate parallel processing.
2. **mapper2.py**: Implements the mapper logic to extract relevant information from music data.
3. **reducer2.py**: Implements the reducer logic to aggregate volumes and counts for each artist and calculate the average volume.
4. **shuffle_main.py**: Driver file that coordinates shuffling and splitting of mapped data for parallel processing.
5. **output_q2.txt**: Output file containing the maximum duration for each artist.

### Usage
1. **Prepare Data**: Ensure that your music data is stored in CSV format and located in a directory accessible to the pipeline.
2. **Execution**: Run the main script `shuffle_main.py` with appropriate command-line arguments to specify input/output directories and other parameters.

### Example
Here's an example of how to run the pipeline:
```bash
python shuffle_main.py /path/to/music_data 4 2 output_q2.txt

/path/to/music_data: Path to the directory containing music data CSV files.
4: Number of mapper processes to use.
2: Number of reducer processes to use.
output_q2.txt: Path to the output file to save the results.
```

### Pseudo Code:

#### split2.py:
```bash
FUNCTION split_file(data_path, num_splits, save_dir):
    Read data from data_path
    Determine batch size based on num_splits (here=20)
    For each batch:
        Split data into chunks
        Save each chunk into save_dir with unique file names
```
#### mapper2.py
```bash
FUNCTION map_data(path):
    Read data from the file at path
    For each line in the file:
        Split the line by comma to extract song title, artist name, and duration
        Append (artist_name, song_duration) tuple to map_output list
    Return map_output
```
#### reducer2.py
```bash
FUNCTION reduce_data(input):
    Initialize an empty dictionary reduced_dict
    For each artist, duration_list pair in input:
        Find the maximum duration in duration_list
        Round the maximum duration to 3 digits
        Add the artist and rounded maximum duration to reduced_dict
    Return reduced_dict
```
#### shuffle_main.py
```bash
FUNCTION shuffle_and_split(map_output, num_splits):
    Group song durations by artist into shuffled_data dictionary
    Split shuffled_data into num_splits chunks
    Return the list of split data

FUNCTION main(data_dir, map_processes, reduce_processes, output_file):
    Read input data files from data_dir
    Apply mapper function to each input file using map_processes
    Combine mapper outputs and shuffle data
    Apply reducer function to shuffled data using reduce_processes
    Write reducer output to output_file
```

