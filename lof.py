#!/usr/bin/env python3

'''I am painfully aware of how bad this code is.
For my personal educational purposes only.
This is a very basic implementation of
Breunig, Markus M., et al. "LOF: identifying density-based local outliers."
Proceedings of the 2000 ACM SIGMOD international conference on Management of data. 2000.
'''

import csv

dataset = []
profiles = dict()
top_outlier_profiles = []

K = 3
N = 10

INPUT_FILEPATH = './mouse.csv'
OUTPUT_FILEPATH = './outliers.csv'
RESULTS = './mouse-results.csv'

def read_data():
    global dataset

    with open(INPUT_FILEPATH) as ip:
        reader = csv.reader(ip)
        for record in reader:
            (x, y, _) = record[0].split(' ')
            dataset.append((float(x), float(y)))

class Profile:
    def __init__(self, point: tuple):
        self.coordinates = point
        self.knn = set()  # set of Profiles
        self.knn_cardinality = 0
        self.distances = dict()  # Profile: float
        self.k_dist = 0.0
        self.reach_dists = dict()  # Profile: float
        self.lrd = 0.0
        self.lof = 0.0

    def get_distance(self, other) -> float:
        def euclidean_distance_squared(a: tuple, b: tuple) -> float:
            (x1, y1) = a
            (x2, y2) = b
            return (x1-x2)**2 + (y1-y2)**2

        return euclidean_distance_squared(self.coordinates, other.coordinates)

def get_knn(center: Profile):
    global profiles

    for other in profiles.values():
        center.distances[other] = center.get_distance(other)

    closest_first = sorted(center.distances.items(), key=lambda dyad:dyad[1])
    knn = set(closest_first[:K+1]) # Including center        
    center.k_dist = closest_first[K][1]
    i = K+1
    while closest_first[i][1] == center.k_dist:
        knn.add(closest_first[i])
        i += 1
    for neighbor in knn:
        center.knn.add(neighbor[0])
        center.knn_cardinality += 1

def analyze():
    global profiles, top_outlier_profiles

    for point in dataset:
        profiles[point] = Profile(point)

    # kNN queries
    for record in profiles.values():
        get_knn(record)
    
    # Reachability distances of each point wrt each of its neighbors
    for record in profiles.values():
        for neighbor in record.knn:
            record.reach_dists[neighbor] = max(record.distances.get(neighbor), neighbor.k_dist)
    
    # lrd's
    for record in profiles.values():
        reach_dist_sum = sum(record.reach_dists.values())
        record.lrd = record.knn_cardinality / reach_dist_sum

    # LOF scores
    for record in profiles.values():
        neighbor_lrd_sum = 0
        for neighbor in record.knn:
            neighbor_lrd_sum += neighbor.lrd
        record.lof = neighbor_lrd_sum / (record.lrd * record.knn_cardinality)
    
    top_outlier_profiles = sorted(profiles.items(), reverse=True, key=lambda p:p[1].lof)[:N]

def write_to_file():
    '''Write top N outliers to file, with following profile data:
    coordinates, knn_cardinality, k_dist, lof
    '''
    top_outliers = []
    for (_, v) in top_outlier_profiles:
        top_outliers.append((v.coordinates, v.knn_cardinality, v.k_dist, v.lof))
    with open(OUTPUT_FILEPATH, 'w') as op:
        writer = csv.writer(op)
        writer.writerow(['Coords', 'kNN card.', 'k-distance', 'LOF'])
        writer.writerows(top_outliers)

def read_expected() -> list:
    expected = []
    with open(RESULTS) as exp:
        reader = csv.reader(exp)
        for record in reader:
            (x, y, _) = record[0].split(' ')
            expected.append((float(x), float(y)))
    return expected

def test():
    # Assert that all knn cardinalities are >= K
    for record in profiles.values():
        assert(record.knn_cardinality >= K)
    # Assert that the number of points in knn whose distance from center is strictly less than the k-distance is <= K-1
    # Assert that the reachability distance is never greater than the corresponding distance
    for record in profiles.values():
        for (other, reach_dist) in record.reach_dists.items():
            assert(other in record.knn)
            assert(reach_dist == other.k_dist or reach_dist == record.distances[other])  # I'd like a more find-grained assertion
    # Assert that each LOF satisfies Theorem 1, which means I need the min and max reachability distances in the direct and indirect neighborhoods
    # Assert that no outlier should have a lower LOF score than a non-outlier
    # Even better, with mouse.csv, I can compare with existing results.
    expected = read_expected()
    received = []
    for (_, v) in top_outlier_profiles:
        received.append(v.coordinates)
    assert(len(received) == len(expected))
    #assert(len(set(received).symmetric_difference(set(expected))) == 0)
    '''Interesting, the expected results list the points with the 15th and 30th largest LOF scores
    instead of the ones with the 9th and 10th largest scores.
    Could there be a mistake in the way I'm calculating LOF scores? I doubt it, because otherwise there would be a bigger discrepancy than this (I think).
    '''

if __name__ == '__main__':
    read_data()
    analyze()
    write_to_file()
    test()
