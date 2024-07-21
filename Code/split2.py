import os 
import pandas as pd 
import sys 

def split_file(data_path, num_splits, save_dir):
    """
    Splits a CSV file into multiple chunks and saves them into the specified directory.
    
    Args:
        data_path (str): Path to the CSV file to be split.
        num_splits (int): Number of chunks to split the file into.
        save_dir (str): Directory where the split files will be saved.
    """
    # Read the data from the CSV file
    data = pd.read_csv(data_path)
    data_size = data.shape[0]

    # Determine the batch size for each split
    batch_size = data_size // num_splits

    # Split the data and save each chunk into separate CSV files
    batch_index = 1
    for start_index in range(0, data_size, batch_size):
        print(f"Running Batch : {batch_index}")
        end_index = min(start_index + batch_size, data_size)
        split_data = data.iloc[start_index:end_index]

        # Save the chunk into a CSV file
        file_path = os.path.join(save_dir, f"split_{batch_index}.csv")
        split_data.to_csv(file_path, index=False)
        batch_index += 1

if __name__ == "__main__":
    """
    Takes user inputs:
    a) data_path: Path to the CSV file to be split.
    b) num_splits: Number of chunks to split the file into.
    c) save_dir: Directory where the split files will be saved.
    """
    data_path = sys.argv[1]
    num_splits = 20  # Splitting into 20 chunks
    save_dir = sys.argv[2]

    assert os.path.exists(data_path), f"{data_path} does not exist"

    os.makedirs(save_dir, exist_ok=True)

    split_file(data_path, num_splits, save_dir)
