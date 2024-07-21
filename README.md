# README: HW2 - Jakob Kuemmerle

## Q1: MapReduce Program for Analyzing Google Books Data 

### Implementation of Q1:
#### mapper.py:
- Analyzes each input line, extracting relevant information such as year, volumes, and words.
- Calculates volumes for specified keywords ('nu', 'chi', 'haw') based on their occurrences in the line.
- Prints the year, keyword, volume, and count to stdout.

#### reducer.py:
- Aggregates volumes and counts for each year and substring combination.
- Calculates the volume average for each year and substring combination.
- Prints the year, substring, and volume average to stdout.

#### run.sh:
- Shell script to execute the MapReduce job using Hadoop Streaming.
- Specifies input paths for both "google_1gram" and "google_2gram" datasets.
- Specifies the output path for the results.
- Uses Hadoop Streaming jar to run the mapper and reducer Python scripts.

### Usage:
1. Ensure that the input datasets ("google_1gram" and "google_2gram") are stored in HDFS.
2. Execute the `run.sh` script to run the MapReduce job.
3. Check the `output.txt` in the specified output directory for the calculated volume averages.

### ChatGPT Promt:

```python
### Task Overview
You have been assigned a task to develop a MapReduce program in Hadoop to analyze Google Books data and calculate the average number of volumes for specific substrings within the dataset. The dataset consists of two parts: "google_1gram" and "google_2gram". It should consider all volumes before 2024 and skip lines where the year is not numerical. Additionally, the length of the parts must be smaller than 4 or bigger than 5.

### Implementation Details

#### Mapper Function:
You are provided with a basic mapper function framework. Your task is to enhance this function to properly extract relevant information from each line of input data and perform substring searches to count occurrences. Ensure that only valid lines with numeric years are considered.

#!/usr/bin/env python3
import sys

# Read each line from stdin
for line in sys.stdin:
    # Remove leading and trailing whitespace and split the words
    words = line.strip().split()

    # Write each word with its count (1) to stdout
    for word in words:
        print(f'{word}\t1')

Reducer Function:

You are provided with a basic reducer function framework. Your task is to enhance this function to aggregate volumes and counts for each year and substring combination, and then calculate the average volume for each combination.

#!/usr/bin/env python3
import sys

current_word = None
current_count = 0

# Read each line from stdin
for line in sys.stdin:
    # Split the input line into word and count
    word, count = line.strip().split('\t')
    # Convert count to integer
    count = int(count)

    # If the current word is not None and is different from the word just read,
    # print the word and its count
    if current_word and current_word != word:
        print(f'{current_word}\t{current_count}')
        current_count = 0

    # Update the current word and increment its count
    current_word = word
    current_count += count

Execution Script:

A shell script is provided to execute the MapReduce job. You need to update the input paths to include both "google_1gram" and "google_2gram" datasets.

#!/bin/bash
input_path1="hw2/google/google_1gram" # in hdfs
input_path2="hw2/google/google_2gram"
output_path="hw2/output1" # in hdfs and must not exist
python_path=$(pwd)
hadoop_lib_path="/opt/hadoop/hadoop/share/hadoop/tools/lib"

yarn jar ${hadoop_lib_path}/hadoop-streaming-2.10.1.jar \
       -files ${python_path}/mapper.py,${python_path}/reducer.py \
    -input ${input_path1},${input_path2} \
    -output ${output_path} \
    -mapper 'python3 mapper.py' \
    -reducer 'python3 reducer.py'

```

## Q2: Music Data Processing Pipeline

### Overview
This repository contains a Python implementation of a data processing pipeline for analyzing music data. The pipeline consists of several stages including splitting, mapping, shuffling, and reducing.

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

