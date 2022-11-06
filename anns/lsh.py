import sys
import csv
from typing import List, Tuple
import random
from collections import defaultdict
import math
import time
import numpy as np
# import faiss

def read_data(file: str) -> List[Tuple[float]]:
    dataset = []
    with open(file) as ip:
        reader = csv.reader(ip)
        for record in reader:
            # for mouse.csv
            (x, y, _) = record[0].split(' ')
            dataset.append((float(x), float(y)))
    return dataset

def generate_hyperplanes(L: int) -> List[Tuple[float]]:
    # generate L pairs of rand floats, these are just lines (d=2)
    planes = []
    for _ in range(L):
        planes.append((random.random() - 0.5, random.random() - 0.5)) # centered around origin
    return planes

def calculate_dot_products(dataset: List[Tuple[float]], planes: List[Tuple[float]]) -> List[List[float]]:
    # dot product each point with the matrix of hyperplanes
    dot_products = []
    for (x, y) in dataset:
        point_products = []
        for (p1, p2) in planes:
            point_products.append(x * p1 + y * p2)
        dot_products.append(point_products)
    # dot_products is a N * L matrix (N = |dataset|)
    return dot_products

def obtain_hashes(dot_products: List[List[float]]) -> List[str]:
    # for each resulting vector, make 0 if negative, 1 otherwise
    hashes = []
    for point_products in dot_products:
        # could be combined into previous steps but whatever
        for i, prod in enumerate(point_products):
            if prod <= 0:
                point_products[i] = 0
            else:
                point_products[i] = 1
    for point_products in dot_products:
        hashes.append(''.join(str(bit) for bit in point_products))
    return hashes

def populate_hashtable(hashes: List[str]) -> dict:  # syntax for better type hint?
    # each vector is a hash key (as string), make hashtable accordingly
    hashtable = defaultdict(list)
    for i, hash in enumerate(hashes):  # list indices are point IDs
        hashtable[hash].append(i)
    return hashtable

def search_kNN(hashtable: dict, hashes: List[str], k: int) -> dict:
    def hamming_distance(a: str, b: str) -> int:
        ans = 0
        for i in range(len(a)):
            if a[i] != b[i]:
                ans += 1
        return ans

    # for each point (O(n)) search only the bucket, i.e. select top k based on hamming distance (compare bit by bit)
    knns = defaultdict(set)
    for i, hash in enumerate(hashes):
        distances = []
        for candidate in hashtable[hash]:
            dist = hamming_distance(hash, hashes[candidate])
            distances.append((candidate, dist))
        distances.sort(key=lambda elt: elt[1])
        knns[i] = set([dist[0] for dist in distances[:k]])  # not quite, as i'll have to add the tied ones too, but close enough
    return knns

def actual_search_kNN(dataset: List[Tuple[float]], k: int) -> dict:
    def euclidean_distance(a: Tuple[float], b: Tuple[float]) -> float:
        (x1, y1) = a
        (x2, y2) = b
        return math.sqrt((x1-x2)**2 + (y1-y2)**2)

    knns = defaultdict(set)
    for i, point in enumerate(dataset):
        distances = []
        for j, other in enumerate(dataset):
            if other == point:
                continue
            distances.append(((i, j), euclidean_distance(point, other)))
        distances.sort(key=lambda elt: elt[1])
        knns[i] = set([dist[0][1] for dist in distances[:k]]) # again, do i consider other points with kdist?
    return knns

def scratch_lsh(L: int, k: int, dataset: list, actual_kNNs: dict) -> tuple: # type
    n = len(dataset)
    tic = time.perf_counter()
    planes = generate_hyperplanes(L)
    dot_products = calculate_dot_products(dataset, planes)
    hashes = obtain_hashes(dot_products)
    hashtable = populate_hashtable(hashes)
    approx_kNNs = search_kNN(hashtable, hashes, k)
    toc = time.perf_counter()
    elapsed = toc - tic
    return elapsed

def np_lsh(L: int, k: int, dataset: list, actual_kNNs: dict) -> tuple:
    # almost copy paste from pinecone
    tic = time.perf_counter()
    plane_norms = np.random.rand(L, 2) - .5  # d = 2
    np_dataset = [np.asarray(vec) for vec in dataset]
    dot_products = [np.dot(vec, plane_norms.T) for vec in np_dataset]
    dot_products = [dp > 0 for dp in dot_products]
    dot_products = [dp.astype(int) for dp in dot_products]
    buckets = {}  # string (hash) to int (point id)
    hashes = []
    for i in range(len(dot_products)):
        hash_str = ''.join(dot_products[i].astype(str))
        hashes.append(hash_str)
        if hash_str not in buckets.keys():
            buckets[hash_str] = []
        buckets[hash_str].append(i)
    approx_kNNs = search_kNN(buckets, hashes, k)
    toc = time.perf_counter()
    elapsed = toc - tic
    return elapsed

def lib_lsh(L: int, k: int, dataset: list, actual_kNNs: dict) -> tuple:
    index = faiss.IndexLSH(2, L)
    index.add(dataset)  # must check data format is good
    # must do this for all points (or all xq) in dataset
    # xq0 = xq[0].reshape(1, d)  # what's 1?
    # we use the search method to find the k nearest vectors
    # D, I = index.search(xq0, k)


def display(elapsed: float, fp_tp_fn: dict):
    print("in", elapsed, "seconds")
    print("achieved results")
    # these results SUCK!
    for (k, v) in fp_tp_fn.items():
        print("point", k, ": (fpr:", v[0], ", dr:", v[1], ", fnr:", v[2], ")")

if __name__ == '__main__':
    [_, L, k, infile] = sys.argv
    dataset = read_data(infile)
    actual_kNNs = actual_search_kNN(dataset, int(k))
    elapsed = scratch_lsh(int(L), int(k), dataset, actual_kNNs)
    print(elapsed)
    elapsed = np_lsh(int(L), int(k), dataset, actual_kNNs)
    print(elapsed)
    # elapsed = lib_lsh(int(L), int(k), dataset, actual_kNNs)
    # print(elapsed)
