import sys
import csv
from typing import Dict, List, Tuple
import random

def read_data(file: str) -> List[Tuple(float)]:  # type hint syntax?
    dataset = []
    with open(file) as ip:
        reader = csv.reader(ip)
        for record in reader:
            # for mouse.csv
            (x, y, _) = record[0].split(' ')
            dataset.append((float(x), float(y)))
    return dataset

def generate_hyperplanes(L: int) -> List[Tuple(float)]:
    # generate L pairs of rand floats, these are just lines (d=2)
    planes = []
    for _ in range(L):
        planes.append((random.random() - 0.5, random.random() - 0.5)) # centered around origin
    return planes

def calculate_dot_products(dataset: List[Tuple(float)], planes: List[Tuple(float)]) -> List[Tuple(float)]:
    # dot product each point with the matrix of hyperplanes
    pass

def obtain_hashes(dot_products: List[Tuple(float)]) -> List[Tuple(str)]:
    # for each resulting vector, make 0 if negative, 1 otherwise (double check that)
    pass

def populate_hashtable(hashes: List[Tuple(str)]) -> dict:  # syntax for better type hint?
    # each vector is a hash key (as string), make hashtable accordingly
    pass

def search_kNN(hashtable: dict) -> dict:
    # for each point (O(n)) search only the bucket, i.e. select top k based on hamming distance (compare bit by bit)
    pass

def calculate_accuracy():
    # calculate FPR and DR
    # wait, how do you actually calculate accuracy?
    pass

def actual_search_kNN(dataset: List[Tuple(float)]) -> dict:
    pass

def main(L: int, k: int, file: str):
    dataset = read_data(file)
    # start stopwatch from here
    planes = generate_hyperplanes(L)
    dot_products = calculate_dot_products(dataset, planes)
    hashes = obtain_hashes(dot_products)
    hashtable = populate_hashtable(hashes)
    approx_kNNs = search_kNN(hashtable)
    # stop stopwatch here
    actual_kNNs = actual_search_kNN(dataset)
    calculate_accuracy(approx_kNNs, actual_kNNs)

if __name__ == '__main__':
    [_, L, k, infile] = sys.argv
    main(L, k, infile)
