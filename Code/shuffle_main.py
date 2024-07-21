import os 
import sys
import mapper2 as mapper2
import reducer2 as reducer2
from collections import defaultdict
from multiprocessing import Pool

def shuffle_and_split(map_output, num_splits):
    """
    Shuffle and Split Logic: Groups song durations by artist and splits the data for parallel processing.
    
    Args:
        map_output (list): Output of the mapper containing artist names and song durations.
        num_splits (int): Number of splits to create.
    
    Returns:
        list: Data split into chunks for parallel processing.
    """
    # Grouping song durations by artist
    shuffled_data = defaultdict(list)
    for artist, duration in map_output:
        shuffled_data[artist].append(duration)

    # Splitting the data into chunks
    batch_size = len(shuffled_data) // num_splits
    split_data = []
    for start_index in range(0, len(shuffled_data), batch_size):
        end_index = min(start_index + batch_size, len(shuffled_data))
        selected_artists = list(shuffled_data.keys())[start_index:end_index]
        temp_data = {artist: shuffled_data[artist] for artist in selected_artists}
        split_data.append(temp_data)
    return split_data 

def main(data_dir, map_processes, reduce_processes, output_file):
    """
    Main function to coordinate map, shuffle, and reduce operations.
    
    Args:
        data_dir (str): Directory where CSV files are located.
        map_processes (int): Number of mapper processes to use.
        reduce_processes (int): Number of reducer processes to use.
        output_file (str): Path to the output file to save the results.
    """
    # Create file paths for CSV files
    file_paths = [os.path.join(data_dir, f) for f in os.listdir(data_dir)]

    # Apply Mapper function to all the file paths
    with Pool(map_processes) as map_pool:
        pool_mapper_output = map_pool.map(mapper2.map_data, file_paths)

    # Combine mapper outputs for shuffle logic
    mapper_output = [item for sublist in pool_mapper_output for item in sublist]

    # Shuffle and split data to apply reduce logic
    shuffled_data = shuffle_and_split(mapper_output, reduce_processes)

    # Apply Reducer function to shuffled data
    with Pool(reduce_processes) as reduce_pool:
        reducer_output = reduce_pool.map(reducer2.reduce_data, shuffled_data)

    # Save reducer output to the output file
    with open(output_file, 'w') as f:
        for item in reducer_output:
            f.write(f"{item}\n")

if __name__ == "__main__":
    """
    Takes user inputs:
    a) data_dir: Directory where CSV files are present.
    b) map_processes: Number of Mapper Processes to use.
    c) reduce_processes: Number of Reducer Processes to use.
    d) output_file: Path to the output file to save the results.
    """
    data_dir = sys.argv[1]
    map_processes = int(sys.argv[2])
    reduce_processes = int(sys.argv[3])
    output_file = sys.argv[4]

    assert os.path.exists(data_dir), f"{data_dir} does not exist"

    main(data_dir, map_processes, reduce_processes, output_file)
