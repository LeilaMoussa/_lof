#!/usr/bin/env python3

'''I am painfully aware of how bad this code is.
For my personal educational purposes only.
This is a very basic implementation of
Breunig, Markus M., et al. "LOF: identifying density-based local outliers."
Proceedings of the 2000 ACM SIGMOD international conference on Management of data. 2000.
'''

import csv
import math

dataset = []
profiles = dict()
top_outlier_profiles = []

K = 3
N = 10
TOTAL = 500

INPUT = './mouse.csv'
OUTPUT_OUTLIERS = './my-outliers.csv'
EXPECTED_OUTLIERS = './expected-outliers.csv'
OUTPUT_PROFILES = './my-profiles.csv'
EXPECTED_PROFILES = './expected-profiles.csv'

def read_data():
    global dataset

    with open(INPUT) as ip:
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
        # p.reach_dists[q] means reach-dist(q, p) because i want the keys to refer to neighbors, not reverse neighbors
        self.reach_dists = dict()  # Profile: float
        self.lrd = 0.0
        self.lof = 0.0

    def get_distance(self, other) -> float:
        def euclidean_distance(a: tuple, b: tuple) -> float:
            (x1, y1) = a
            (x2, y2) = b
            return math.sqrt((x1-x2)**2 + (y1-y2)**2)

        return euclidean_distance(self.coordinates, other.coordinates)

def get_knn(center: Profile):
    global profiles

    for other in profiles.values():
        if center.distances.get(other):
            continue
        d = center.get_distance(other)
        center.distances[other] = d
        other.distances[center] = d

    closest_first = sorted(center.distances.items(), key=lambda dyad:dyad[1])
    knn = set(closest_first[:K+1]) # Including center at first
    knn.remove((center, 0))  # of course, hoping the distance would be 0 as it should be
    assert(len(knn) == K)
    center.k_dist = closest_first[-1][1]
    i = K
    while i < TOTAL and closest_first[i][1] == center.k_dist:
        knn.add(closest_first[i])
        i += 1
    assert(i >= K)
    assert(len(knn) >= K)
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
    
    # Reachability distances of each point wrt all other points
    for record in profiles.values():
        #for neighbor in record.knn:
        for neighbor in profiles.values():  # inaccurate naming
            # it's the reach dist of the neighbor wrt to the center, not the opposite
            # and we should be looking at the kdist of the center, not the neighbor
            # reach-dist(neighbor, center) = max { kdist(center), d(neighbor, center) }
            record.reach_dists[neighbor] = max(record.distances.get(neighbor), record.k_dist)
    
    # lrd's
    # i didn't fix reach dists here
    # formula: i need reach-dist(point, neighbor)
    # so use neighbor.reach_dists[point]
    for record in profiles.values():
        # assert(len(record.reach_dists.values()) == record.knn_cardinality)
        # reach_dist_sum = sum(record.reach_dists.values())
        reach_dist_sum = 0
        for neighbor in record.knn:
            reach_dist_sum += neighbor.reach_dists.get(record)
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
    # write the full(-ish) profiles first
    abridged_profiles = []
    for p in profiles.values():
        abridged_profiles.append((p.coordinates, p.knn_cardinality, p.k_dist, p.lrd, p.lof))
    with open(OUTPUT_PROFILES, 'w') as op:
        writer = csv.writer(op)
        writer.writerow(['Coords', 'kNN card.', 'k-distance', 'lrd', 'LOF'])
        writer.writerows(abridged_profiles)
    # then the outliers
    top_outliers = []
    for (_, v) in top_outlier_profiles:
        top_outliers.append((v.coordinates, v.knn_cardinality, v.k_dist, v.lof))
    with open(OUTPUT_OUTLIERS, 'w') as op:
        writer = csv.writer(op)
        writer.writerow(['Outlier coords', 'kNN card.', 'k-distance', 'LOF'])
        writer.writerows(top_outliers)

def read_expected_outliers() -> set:
    expected = set()
    with open(EXPECTED_OUTLIERS) as exp:
        reader = csv.reader(exp)
        for record in reader:
            (x, y, _) = record[0].split(' ')
            expected.add((float(x), float(y)))
    return expected

def read_expected_profiles() -> set:
    expected = set()
    with open(EXPECTED_PROFILES) as exp:
        reader = csv.reader(exp)
        for record in reader:
            (_, x, y, _, lof) = record[0].split(' ')
            lof = lof.split('=')[1]
            expected.add((float(x), float(y), float(lof)))
    return expected

def test():
    # definition of k-distance
    for record in profiles.values():
        assert(record.knn_cardinality >= K)
        cnt = 0
        c = 0
        for neighbor in record.knn:
            if record.distances.get(neighbor) < record.k_dist:
                cnt += 1
            if record.distances.get(neighbor) <= record.k_dist:
                c += 1
        assert(c == record.knn_cardinality)
        # assert(cnt <= K-1)
    for record in profiles.values():
        for (other, reach_dist) in record.reach_dists.items():
            assert(reach_dist == record.k_dist or reach_dist == record.distances.get(other))
            assert(reach_dist == max(record.k_dist, record.distances.get(other)))
            if other in record.knn or other is record:
                assert(reach_dist == record.k_dist)
            else:
                pass
                # assert(reach_dist == record.distances.get(other))
            # assert(other in record.knn)
            assert(reach_dist == record.k_dist or reach_dist == record.distances.get(other))  # I'd like a more find-grained assertion
    # Assert that each LOF satisfies Theorem 1, which means I need the min and max reachability distances in the direct and indirect neighborhoods
    # Assert that no outlier should have a lower LOF score than a non-outlier
    # Even better, with mouse.csv, I can compare with existing results.
    expected = read_expected_outliers()
    received = set()
    for (_, v) in top_outlier_profiles:
        received.add(v.coordinates)
    assert(len(received) == len(expected))
    print("len of difference in outliers", len(received.symmetric_difference(expected)))
    #assert(len(received.symmetric_difference(expected)) == 0)
    # EVEN better, test `profiles` against EXPECTED_PROFILES
    expected = read_expected_profiles()  # set of tuples of x, y, lof
    received = set()
    for p in profiles.values():
        received.add((p.coordinates[0], p.coordinates[1], p.lof))
    assert(len(received) == len(expected))
    #assert(len(received.symmetric_difference(expected)) == 0)
    print("len of difference in profiles", len(received.symmetric_difference(expected)))
    for record in profiles.values():
        for neighbor in record.knn:
            assert(neighbor != record)  # not the same point, but it's okay if it's a different point with the same coordinates

if __name__ == '__main__':
    read_data()
    analyze()
    write_to_file()
    test()
