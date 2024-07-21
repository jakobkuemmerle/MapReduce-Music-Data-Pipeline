def reduce_data(input):
    """
    This function takes in the mapped data and returns the max duration for each artist as a dict.
    The duration is rounded to 3 digits.
    """
    # initiate empty dict, iterate through all artists and add the avg duration as value
    reduced_dict = {}
    
    for key, value_list in input.items():
        reduced_dict[key] = round(max(value_list),3)
    return reduced_dict
