import time
import os
import csv
import numpy as np
import multiprocessing
from pandas import read_csv
from fuzzywuzzy import fuzz
from time import process_time

start_time = process_time()

# input definition
acquirers_csv = "input_acquirers.csv"
bank_names_csv = "input_bank_names.csv"

# read data from csv file
acquirers_data = read_csv(acquirers_csv, index_col=None, header=0)
acquirers_names = acquirers_data.values[:, 2]

bank_names_data = read_csv(bank_names_csv, index_col=0, header=0)
bank_names = bank_names_data.values[:, 0]

# array to list
bank_names = bank_names.tolist()
acquirers_names = acquirers_names.tolist()

# csv_output definition
output_csv = "fuzzy_output.csv"
file = open(output_csv, "a", newline='')
writer = csv.writer(file)


# store the values to csv
def write2csv(row):
    writer.writerow(row)
    

# define a function for fuzzy match between each acquirer and bank_names, and return their similarities
def fuzzy_match(x):
    similarities = []
    for bank_name in bank_names:
        value = fuzz.token_sort_ratio(acquirers_names[x], bank_name)
        similarities.append(value)
    
    # get those five bank_names with the highest similarities
    similarities = np.array(similarities)
    
    indices = np.argsort(similarities)  # return the indices of list from the smallest to largest
    five_highest_indices = indices[len(indices)-5:]
    
    five_highest_bank_names = [acquirers_names[x]]
    
    for n in range(len(five_highest_indices)):
        five_highest_bank_names.append(bank_names[five_highest_indices[-(n+1)]])
    
    # save to csv file
    write2csv(five_highest_bank_names)
    

if __name__ == '__main__':
    # multiple processes
    multiprocessing.freeze_support()
    pool = multiprocessing.Pool(processes=multiprocessing.cpu_count()-1)  # multiprocessing.cpu_count()-1 
    for x in range(len(acquirers_names)):
        pool.apply_async(fuzzy_match, (x,))

    pool.close()  # close process pool
    pool.join()  # waiting for all those processes finished and the line must be defined after pool.close()
    
    output_data = read_csv(output_csv, index_col=None, header=None)
    new_output_data = output_data.values[:, :]
    
    # clear the raw data in the csv file
    file.truncate()
    
    # save to the new csv file and write the header name
    writer.writerow(["Acquirer", "1", "2", "3", "4", "5"])
    for n in range(len(new_output_data)):
        writer.writerow(new_output_data[n])
    
    end_time = process_time()
    print('Computation time = ', end_time - start_time, 's!')
