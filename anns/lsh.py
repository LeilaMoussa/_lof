import sys
import csv
from typing import List, Tuple
import random
from collections import defaultdict
import math
import time

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

def calculate_accuracy(approx_kNNs: dict, actual_kNNs: dict, n: int) -> dict:
    # calculate FPR and DR (?)
    fp_tp_fn = defaultdict(tuple)
    for i in range(n):
        fp_tp_fn[i] = (
            # BAD
            # fpr = points in approx that aren't in actual
            len(approx_kNNs[i].difference(actual_kNNs[i])),
            # dr = intersection
            len(approx_kNNs[i].intersection(actual_kNNs[i])),
            # fnr = points in actual but not in approx
            len(actual_kNNs[i].difference(approx_kNNs[i]))
        )
    return fp_tp_fn

def main(L: int, k: int, file: str) -> tuple: # type
    dataset = read_data(file)
    n = len(dataset)
    tic = time.perf_counter()
    planes = generate_hyperplanes(L)
    dot_products = calculate_dot_products(dataset, planes)
    hashes = obtain_hashes(dot_products)
    hashtable = populate_hashtable(hashes)
    approx_kNNs = search_kNN(hashtable, hashes, k)
    toc = time.perf_counter()
    elapsed = toc - tic
    actual_kNNs = actual_search_kNN(dataset, k)
    fp_tp_fn = calculate_accuracy(approx_kNNs, actual_kNNs, n)
    return elapsed, fp_tp_fn

if __name__ == '__main__':
    [_, L, k, infile] = sys.argv
    elapsed, fp_tp_fn = main(int(L), int(k), infile)
    print("in", elapsed, "seconds")
    print("achieved results")
    # these results SUCK!
    for (k, v) in fp_tp_fn.items():
        print("point", k, ": (fpr:", v[0], ", dr:", v[1], ", fnr:", v[2] + ")")
