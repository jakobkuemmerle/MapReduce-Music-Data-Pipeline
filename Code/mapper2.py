import os 
import sys 

def map_data(path):
    """
    Mapper logic : Map each artist with duration for each song
    args:
    data_path : paths of the music data split
    """
    # Take the path and read the data
    file = open(path,'r')
    data_shuffled = file.readlines()

    # Map each artist with corresponding Sound Duration
    map_output = []
    for line in data_shuffled:
        line = line.strip().split(',')
        # Fetch song title, artist name and duration
        try:
            song_title = line[0]
            artist_name = line[2]
            song_duration = float(line[3])
        except ValueError as e:
            pass
        map_output.append((artist_name, song_duration))

    file.close()
    return map_output
