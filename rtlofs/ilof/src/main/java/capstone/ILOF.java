/**
 * Pokrajac, D., Lazarevic, A., & Latecki, L. J. (2007, March). 
 * Incremental local outlier detection for data streams. 
 * In 2007 IEEE symposium on computational intelligence and data mining (pp. 504-515). 
 * IEEE.
 * 
 * Author: Leila Moussa (l.moussa@aui.ma)
 */

package capstone;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Printed;

import com.google.common.collect.MinMaxPriorityQueue;
import com.google.common.collect.Sets;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
//import java.util.Map.Entry;
import java.util.PriorityQueue;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.ArrayList;
import java.util.Optional;

import org.javatuples.Pair;
import org.javatuples.Triplet;

import io.github.cdimascio.dotenv.Dotenv;

import be.tarsos.lsh.CommandLineInterface;
import be.tarsos.lsh.*;
import be.tarsos.lsh.families.*;
import net.sf.javaml.core.kdtree.KDTree;

public class ILOF {

  // IMPROVE: Conditionally initialize these collections depending on
  // whether ILOF is called from Driver or from another algorithm
  // For example blackholes and VP related collections are only relevant (again, as of now) in RLOF.

  public static HashSet<Point> pointStore; // The collection of points in ILOF memory, and doubles as window in RLOF when ILOF subprocess is called
  public static HashMap<HashSet<Point>, Double> symDistances; // Symmetric distances (MAN, EUC, etc.), where each key is a hashet of size 2
  public static HashMap<Point, PriorityQueue<Pair<Point, Double>>> kNNs; // Each key is a point, and the value is a max heap of neighbors
                                                                          // sorted by distance, so it's easy to kick out the farthest neighbors later
  public static HashMap<Point, Double> kDistances; // each point has a k-distance
  public static HashMap<Pair<Point, Point>, Double> reachDistances; // each ordered pair of points has a reachability distance
  public static HashMap<Point, Double> LRDs; // each point has a local reachability density and local outlier factor
  public static HashMap<Point, Double> LOFs;

  public static HashSet<Triplet<Point, Double, Integer>> blackHoles; // in RLOF, we keep track of each summarized area or blackhole
                                                                      // defined by a center, a radius, and the number of deleted points that used to populate the area
  public static HashMap<Point, Double> vpKdists; // in RLOF, each set of VPs shares a k-distance and LRD
  public static HashMap<Point, Double> vpLrds; // the key is the center of the corresponding blackhole
                                                // these 3 RLOF-related collections are only read here in ILOF
                                                // in fact, these 3 collections are aliases for the ones in RLOF
                                                // not the best design, I know...

  public static int k;
  public static int d;
  public static int TOP_N; // A fixed number of outliers to flag, depends on the dataset and is provided by the user in .env
  public static String DISTANCE_MEASURE;
  public static String NNS_TECHNIQUE;
  public static int HASHES;
  public static int HASHTABLES;
  public static int HYPERPLANES;
  public static int V;
  public static String SINK; // name of sink file, determined based on parameters

  public static MinMaxPriorityQueue<Pair<Point, Double>> topOutliers; // Incrementally record top outliers
  public static long totalPoints; // number of points inserted so far
  public static long startTime; // time stamp corresponding to consumption of first point

  // R = |repetitions|, H = |hyperplanes in each repetition|
  public static ArrayList<ArrayList<ArrayList<Double>>> hyperplanes; // R sets of H hyperplanes, each expressed as a d-dimensional vector
  public static ArrayList<HashMap<Long, HashSet<Point>>> hashTables; // R tables, with key being binary hash and value being set of points sharing that hash in that iteration
  public static HashMap<Point, ArrayList<Long>> hashes; // inverse map of hashTables

  public static KDTree kdindex; // KD-tree used when NNS_TECHNIQUE is KD

  public static void getKDkNN(Point point) {
    // Instantiate tree first time
    if (kdindex == null) {
      kdindex = new KDTree(d); // In a KD Tree, K is the dimensionality
    }
    // IMPROVE: maybe it would just be better to change Point#attributes from ArrayList to double[] to avoid this extra overhead
    double[] attributes = point.attributesToArray();
    // KDTree throws an error when you query more points than exist in the tree
    int n_neighs = Math.min(k, kdindex.count());
    Object[] ans = new Object[n_neighs];
    if (n_neighs > 0) {
      ans = kdindex.nearest(attributes, n_neighs); // here's the actuall kdtree query
    }
    // ans, if nonempty, contains the list of exactly n_neighs neighbors in ASCENDING order of distance
    // according to Euclidean distance (see implementation uses a mix of euclidean and euclidean squared distance) --
    // I haven't quite dug into the details...
    // Make a max heap to contain kNN
    PriorityQueue<Pair<Point, Double>> pq = new PriorityQueue<>(Comparators.pointComparator().reversed());
    Double kdist = Double.POSITIVE_INFINITY; // The default value of k-distance is +INF (if |kNN|=0)
    for (int i = 0; i < ans.length; i++) {
      Point other = (Point)ans[i];
      // IMPROVE: use symDistances here to avoid too repeated distance calculations

      // Although KDTree has a fixed distance measure
      // I have to stay consistent with my own distance measure throughout execution
      // because distances are calculated in other places as well
      // so while KDTree gives me the right order of neighbors, I unfortunately cannot use it to not risk mixing up distance measures
      // IMPROVE: leverage order of ans to avoid calculating distances here
      Double distance = point.getDistanceTo(other, DISTANCE_MEASURE);
      if (i == ans.length - 1) {
        // Here i'm leveraging that order: the last element is the farthest, so distance is point's k-distance
        kdist = distance;
      }
      pq.add(new Pair<Point, Double>(other, distance));
    }
    kNNs.put(point, pq);
    kDistances.put(point, kdist);
    // Insert the point last into the kdtree, because self cannot be in kNN
    // TODO: oops I just realized that because this function may execute multiple times for the same point
    // a) it may insert the same point multiple times and I don't know how this impl of KDTree handles duplicates
    // b) it may actually result in self being in kNN
    // so we really need to a) double check insertion logic to avoid duplicates and b) query for k+1
    // points and remove the closest.
    kdindex.insert(attributes, point);
  }

  // IMPROVE: may want to change visibility of some things to private, etc.
  public static int hammingDistance(long hasha, long hashb) {
    // the number of ones in hasha XOR hashb is the hamming distance (# of different bits at same position)
    long xor = hasha ^ hashb;
    int ans = 0;
    // count # of 1's
    while (xor > 0) {
        if (xor % 2 == 1) ans++;
        xor = xor >> 1;
    }
    return ans;
  }

  // This function does 3 things:
  // calculate all hashes of a point, write them to memory, and return the search space for that point
  // the first 2 things need not be done more than once (see TODO below)
  // while 3rd thing needs to be done every time
  public static HashSet<Point> hashAndSave(Point point) {
    // each point has R hashes (as many hashes as LSH repetitions)
    if (hashes.containsKey(point) == false) {
      hashes.put(point, new ArrayList<Long>(HASHTABLES));
    }
    // the reduces search space, consisting of the candidates for kNN
    HashSet<Point> searchSpace = new HashSet<>();
    for (int iter = 0; iter < HASHTABLES; iter++) {
      ArrayList<ArrayList<Double>> iteration = hyperplanes.get(iter); // iteration is LSH repetition
      // treat hash as a binary number for speed and smaller size in memory (compared to String for example)
      // start with hash of 0 before constructing it based on the hyperplanes of that particular iteration or repetition
      long hash = 0b0;
      for (ArrayList<Double> hyperplane: iteration) { // for each hyperplane
        double dot = 0; // get the dot product of the hyperplane and the point
        for (int i = 0; i < d; i++) {
          dot += point.getAttribute(i) * hyperplane.get(i);
        }
        // if dot product > 0, the next bit is 1, to be added to right of the hash
        // else, add 0
        // left shift hash and set the lsb if bit is 1
        hash = hash << 1;
        if (dot > 0) hash |= 1;
      }
      // save (hash: point) in corresponding table in hashTables
      // and save (point: hash) in hashes
      // if not already there
      // i'm checking that condition because i don't want to keep adding the same hash over and over again

      // TODO: this function incurs a lot of repeated calculation because it was written before i created the collection `hashes`
      // Instead, we need to return searchSpace for each hash in hashes.get(point)
      // unless the point hasn't been assigned all its hashes yet
      // so do something like this:
      /* if (hashes.containsKey(point) && hashes.get(point).size() == HASHTABLES) {
        int iter = 0;
        for (Long iter_hash : hashes.get(point)) {
          searchSpace.addAll(hashTables.get(iter).get(iter_hash));
          iter++; // this would work because hashes and hashTables follow the same order (are parallel)
        }
      } else {
        // do all the calculations above and save each hash to memory, and build up searchSpace similarly
      } */

      if (hashTables.get(iter).containsKey(hash) == false) {
        hashTables.get(iter).put(hash, new HashSet<>());
      } else {
        // while at it, fill searchSpace with the existing points in that hashmap entry, before saving current point
        searchSpace.addAll(hashTables.get(iter).get(hash));
      }
      if (hashes.get(point).size() < HASHTABLES) {
        hashTables.get(iter).get(hash).add(point);
        hashes.get(point).add(hash);
      }
    }
    return searchSpace;
  }

  // This function is my implementation of LSH with random projection from scratch, no bells or whistles, just my bare understanding of it.
  public static void getLshkNN(Point point) {
    // statically generate R sets of H hyperplanes
    // for each new point, get its dot products with the hyperplanes, then its hash
    // statically keep R hashtables, each with H-bit hashes
    // end up with R buckets that the new point belongs to
    // the search space is all the other points in all these buckets

    // each hyperplane is defined by a normal vector of d dimensions
    // generate H vectors of d dimensions, where each element is in range 0,1, then subtract .5 to center around 0
    // (might want to think about normalizing input data)

    // Generate these hyperplanes once
    if (hyperplanes == null) {
      hyperplanes = new ArrayList<>(HASHTABLES);
      hashTables = new ArrayList<>(HASHTABLES);
      for (int i = 0; i < HASHTABLES; i++) {
        hashTables.add(new HashMap<>());
        ArrayList<ArrayList<Double>> iteration = new ArrayList<>(HYPERPLANES);
        for (int j = 0; j < HYPERPLANES; j++) {
          ArrayList<Double> norm = new ArrayList<>(d);
          for (int m = 0; m < d; m++) {
            norm.add(
              Math.random() - 0.5
            );
          }
          iteration.add(norm);
        }
        hyperplanes.add(iteration);
      }
      hashes = new HashMap<>();
    }

    // If in RLOF and VPs exist, hash them too because they may be neighbors
    if (blackHoles != null && blackHoles.size() > 0) {
      ArrayList<VPoint> vps = deriveVirtualPoints();
      vps.forEach(x -> hashAndSave(x));
    }

    HashSet<Point> searchSpace = hashAndSave(point);

    // here, should have avg of R * (N / 2**H) points in searchSpace where N = number of Points and VPoints in window
    // for each, get distance using distance measure, then do similar logic as in getFlatkNN()

    // At this point, seachSpace may be empty in at least 2 cases:
    // a) if totalPoints == 1
    // b) if none of the point's hashes are shared with any other point
    // in either case (or some third case i don't know about),
    // and since the hashes are final, the point will never get neighbors which will result in k-distance=reach-dist=+INF, lof=NaN, etc.
    // so do the following to guarantee the point gets at least k neighbors somehow:
    // for each hash of the point, get k other hashes that are closest to it according to Hamming Distance
    // for example hamming(000, 011) = 2 and hamming(111, 111) = 0
    // each closest hash corresponds to one bucket of points who have been assigned that hash in some iteration
    // the total size of all these buckets is k * (N / 2**H)
    // from which you need at least k as neighbors
    // so incrementally add each bucket to searchSpace until you reach at least k
    // the granularity is a bucket, because within a single bucket, points are considered equidistant (you can't tell which is closer within the same bucket
    // so you pick them all up at once until you get >=k points)

    // IMPROVE: I'm just realizing that I could have put all k buckets in searchSpace and left it up to getFlatkNNFromSearchSpace()
    // to get a more accurate kNN
    // so really, no need to incrementally add buckets to searchSpace until k is reached
    // as getFlatkNNFromSearchSpace() takes care of |kNN|

    if (searchSpace.isEmpty() && pointStore.size() > 1) {
      for (int i = 0; i < HASHTABLES; i++) {
        long hash = hashes.get(point).get(i);
        HashMap<Long, HashSet<Point>> currentTable = hashTables.get(i);
        HashSet<Long> otherHashes = new HashSet<>(currentTable.keySet());
        PriorityQueue<Pair<Long, Integer>> sortedHashesByHamming = new PriorityQueue<>(Comparators.hashComparator().reversed());
        otherHashes.forEach(otherHash -> {
          sortedHashesByHamming.add(new Pair<Long, Integer>(otherHash, hammingDistance(hash, otherHash)));
        });
        // now all other hashes in that table are sorted in a min heap
        // keep polling and check size so far against k
        while (sortedHashesByHamming.size() > 0 && searchSpace.size() < k) {
          long nextClosestHash = sortedHashesByHamming.poll().getValue0();
          searchSpace.addAll(currentTable.get(nextClosestHash));
        }
      }
    }

    // NOTE: for all assertions, you need to run the application with the java flag -ea for enable assertions
    // otherwise, they are not executed!
    // Make sure you disable assertions in benchmarks!
    assert(seachSpace.size() > 0);

    kNNs.put(point, getFlatkNNFromSearchSpace(point, searchSpace));
  }

  // This function uses TarsosLSH, first transforming search space to a list of be.tarsos.lsh.Vector
  public static void getTarsosLshkNN(Point point) {
    HashFamily hashFamily = null;
    // dataset is the pointStore as Vectors, excluding current point
    List<Vector> dataset = Sets.difference(pointStore, new HashSet<Point>(Arrays.asList(point))).stream().map(Point::toVector).collect(Collectors.toList());
    assert(Tests.pointNotInDataset(point, dataset));
    if (blackHoles != null && blackHoles.size() > 0) {
      // add virtual points too
      dataset.addAll(deriveVirtualPoints().stream().map(Point::toVector).collect(Collectors.toList()));
    }
    assert(Tests.expectVirtualPointInDataset(dataset, blackHoles.size(), d));
    switch (DISTANCE_MEASURE) {
      case "EUCLIDEAN":
        int radiusEuclidean = (int)Math.ceil(LSH.determineRadius(dataset, new EuclideanDistance(), 40));
        hashFamily = new EuclidianHashFamily(radiusEuclidean, d);
        break;
      case "MANHATTAN":
        int radiusCityBlock = (int)Math.ceil(LSH.determineRadius(dataset, new CityBlockDistance(), 40));
        hashFamily = new CityBlockHashFamily(radiusCityBlock, d);
        break;
      // IMPROVE: implement proper logging everywhere in this project: stdout logging + output and error log files
      default: System.out.println("Unsupported distance measure.");
    }

    // Neighbors could be Points or VPoints.
    List<Point> neighbors = CommandLineInterface.lshSearch(dataset, // actual TarsosLSH query
                            hashFamily,
                            HASHES,
                            HASHTABLES,
                            Arrays.asList(point.toVector()),
                            k)
                            .get(0)
                            .stream()
                            .map(Point::fromVector) // turn back from Vectors to Points
                            .collect(Collectors.toList());

    assert(Tests.pointNotInNeighbors(point, neighbors));

    // IMPROVE: try to guarantee a minimum number of neighbors from Tarsos.

    PriorityQueue<Pair<Point, Double>> pq = new PriorityQueue<>(Comparators.pointComparator().reversed());
    for (Point n : neighbors) {
      // NOTE: The next 6 lines of code are repeated in a lot of places
      // and serve to avoid repeating distance calculations for the same pair of points
      // but, note that distance calculation costs O(d), making both lookup and calculation from scratch O(1) operations
      // the question is whether the difference in the constant factor justifies this much code bloat, and more importantly,
      // a whole other collection (symDistances)
      // that's something experiments (perhaps isolated experiments) will help us discover!
      Double dist;
      if (symDistances.containsKey(new HashSet<Point>(Arrays.asList(point, n)))) {
        dist = symDistances.get(new HashSet<Point>(Arrays.asList(point, n)));
      } else {
        dist = point.getDistanceTo(n, DISTANCE_MEASURE);
        symDistances.put(new HashSet<Point>(Arrays.asList(point, n)), dist);
      }
      pq.add(new Pair<Point, Double>(n, dist));
    }
    assert(Tests.isMaxHeap(pq));
    kNNs.put(point, pq);
    kDistances.put(point, pq.size() == 0 ? Double.POSITIVE_INFINITY : pq.peek().getValue1());
  }

  // Each blackhole is delimited by V = 2 * d VPs
  public static ArrayList<VPoint> deriveVirtualPointsFromBlackhole(Triplet<Point, Double, Integer> bh) {
    ArrayList<VPoint> ans = new ArrayList<>();
    if (bh == null) return ans;
    for (int pl = 0; pl < d; pl++) { // for each line drawn along each of the d axes
      for (int pos = 0; pos < 2; pos++) { // there are 2 intersections with the hypersphere bounding the kNN which becomes a blackhole
        ans.add(new VPoint(bh.getValue0(), bh.getValue1(), d, pl, pos));
      }
    }
    return ans;
  }

  // IMPROVE: This function calculates all the VPs corresponding to all the blackholes in memory at that moment
  // This process may happen often, in fact too often to be running BH * V operations
  // so it might be better to write the VPs as they are to memory and just keep using that collection everywhere
  // The reason I didn't start by doing that is the fear that the instantiated virtual point hashset would be bigger than
  // the deleted points, making summarization useless
  // but i realize that this decision was probably made at the expense of speed and code readability
  // and that a good selection of parameters should circumvent that fear.
  public static ArrayList<VPoint> deriveVirtualPoints() {
    ArrayList<VPoint> ans = new ArrayList<>();
    try {
      blackHoles.forEach(bh -> {
        ans.addAll(deriveVirtualPointsFromBlackhole(bh));
      });
    } catch (Exception e) {
      System.out.println("deriveVirtualPoints " + e + " " + e.getStackTrace()[0].getLineNumber());
    }
    assert(Tests.isEq(ans.size(), blackHoles.size() * d * 2));
    return ans;
  }

  // almost fully copy paste from getFlatkNN, might want to refactor and keep only one
  // this function is a generalization of getFlatkNN, where the searchSpace can be anything, not just pointStore
  public static PriorityQueue<Pair<Point, Double>> getFlatkNNFromSearchSpace(Point point, HashSet<Point> searchSpace) {  // searchSpace is candidate Points and VPoints
    try {
      ArrayList<Pair<Point, Double>> distances = new ArrayList<>();
      searchSpace.forEach(otherPoint -> {  // otherPoint may be Point or VPoint
        if (otherPoint.equals(point)) return;
        Double dist;
        if (symDistances.containsKey(new HashSet<Point>(Arrays.asList(point, otherPoint)))) {
          dist = symDistances.get(new HashSet<Point>(Arrays.asList(point, otherPoint)));
        } else {
          dist = point.getDistanceTo(otherPoint, DISTANCE_MEASURE);
          symDistances.put(new HashSet<Point>(Arrays.asList(point, otherPoint)), dist);
        }
        distances.add(new Pair<Point, Double>(otherPoint, dist));
      });
      assert(Tests.isEq(distances.size(), pointStore.size() - 1  + (blackHoles != null ? blackHoles.size() * 2 * d : 0)));
      distances.sort(Comparators.pointComparator());
      if (distances.size() >= 2) {
        assert(Tests.isSortedAscending(distances));
      }
      Double kdist = 0.0;
      if (distances.size() > 0) {
        kdist = distances.get(Math.min(k-1, distances.size()-1)).getValue1();
      }
      // TODO: kNNs.get(point).put(pq); // and then return void instead of returning the pq.
      kDistances.put(point, kdist == 0 ? Double.POSITIVE_INFINITY : kdist);
      int i = k;
      for (; i < distances.size() && distances.get(i).getValue1().equals(kdist); i++) { }
      PriorityQueue<Pair<Point, Double>> pq = new PriorityQueue<>(Comparators.pointComparator().reversed());
      if (distances.size() > 0) {
        pq.addAll(distances.subList(0, Math.min(i, distances.size())));
      }
      assert(Tests.isMaxHeap(new PriorityQueue<Pair<Point, Double>>(pq)));
      if (totalPoints > k) {
        assert(Tests.atLeastKNeighbors(kNNs.get(point), k));
      }
      return pq;
    } catch (Exception e) {
      System.out.println("getFlatkNNFromSearchSpace " + e + " " + e.getStackTrace()[0].getLineNumber());
    }
    return null;
  }

  // This function simply performed linear search on pointStore, finding >= k neighbors.
  public static void getFlatkNN(Point point) {
    try {
      ArrayList<Pair<Point, Double>> distances = new ArrayList<>();
      // search space is all the active points in memory
      HashSet<Point> searchSpace = new HashSet<>(pointStore);
      if (blackHoles != null && blackHoles.size() > 0) {
        // and any virtual points, if in RLOF
        searchSpace.addAll(deriveVirtualPoints());
      }
      searchSpace.forEach(otherPoint -> {  // otherPoint may be Point or VPoint
        if (otherPoint.equals(point)) return;
        Double dist;
        if (symDistances.containsKey(new HashSet<Point>(Arrays.asList(point, otherPoint)))) {
          dist = symDistances.get(new HashSet<Point>(Arrays.asList(point, otherPoint)));
        } else {
          dist = point.getDistanceTo(otherPoint, DISTANCE_MEASURE);
          symDistances.put(new HashSet<Point>(Arrays.asList(point, otherPoint)), dist);
        }
        distances.add(new Pair<Point, Double>(otherPoint, dist));
      });
      assert(Tests.isEq(distances.size(), pointStore.size() - 1  + (blackHoles != null ? blackHoles.size() * 2 * d : 0)));
      distances.sort(Comparators.pointComparator());
      if (distances.size() >= 2) {
        assert(Tests.isSortedAscending(distances));
      }
      Double kdist = 0.0;
      if (distances.size() > 0) {
        // need to use Math.min here to avoid index out of bounds exception in edge cases
        kdist = distances.get(Math.min(k-1, distances.size()-1)).getValue1();
      }
      kDistances.put(point, kdist == 0 ? Double.POSITIVE_INFINITY : kdist); // could have just initialized kdist to +INF to start with
      // now find the points on the perimeter of the kNN, each at the same distance kdist from point
      // starting from index k of the array distances, keep going until they points are farther than kdist
      int i = k;
      // NOTE: a major point of confusion and surprise for me was comparison of Doubles vs doubles
      // so pay attention to that!
      for (; i < distances.size() && distances.get(i).getValue1().equals(kdist); i++) { }
      // at this point, i is the index of the last neighbor
      PriorityQueue<Pair<Point, Double>> pq = new PriorityQueue<>(Comparators.pointComparator().reversed());
      if (distances.size() > 0) {
        pq.addAll(distances.subList(0, Math.min(i, distances.size())));
      }
      assert(Tests.isMaxHeap(new PriorityQueue<Pair<Point, Double>>(pq)));
      kNNs.put(point, pq);
      if (totalPoints > k) {
        assert(Tests.atLeastKNeighbors(kNNs.get(point), k));
      }
    } catch (Exception e) {
      // pretty janky way of logging errors.... hence the need for nicer logging
      // sometimes it's better to print the full stacktrace
      System.out.println("getFlatkNN " + e + " " + e.getStackTrace()[0].getLineNumber());
    }
  }

  public static void getkNN(Point point, String NNS_TECHNIQUE) {
    switch (NNS_TECHNIQUE) {
      case "FLAT": getFlatkNN(point); return;
      case "KD": getKDkNN(point); return;
      case "LSH": getLshkNN(point); return;
      case "TARSOS": getTarsosLshkNN(point); return;
      default: System.out.println("Unsupported nearest neighbor search technique.");
    }
  }

  public static void getRds(Point point) {
    try {
      kNNs.get(point).forEach(neighborpair -> {
        Point neighbor = neighborpair.getValue0();
        Double kdist;
        // k-distances may be stored in vpKdists is the point is actually a VPoint
        // else, if real point, just find it in kDistances
        // this duality of collections may get unwieldy, bloated, and error prone
        // so we might want to find a smart way to tackle that
        // NOTE: to check if a super Point is actually a VPoint, you must use getClass, not instanceof
        if (neighbor.getClass().equals(VPoint.class)) {
          kdist = vpKdists.get(((VPoint)neighbor).center);
        } else {
          kdist = kDistances.get(neighbor);
        }
        Double dist;
        if (symDistances.containsKey(new HashSet<Point>(Arrays.asList(point, neighbor)))) {
          dist = symDistances.get(new HashSet<Point>(Arrays.asList(point, neighbor)));
        } else {
          dist = point.getDistanceTo(neighbor, DISTANCE_MEASURE);
          symDistances.put(new HashSet<Point>(Arrays.asList(point, neighbor)), dist);
        }
        Double reachDist = Math.max(kdist, dist);
        Pair<Point, Point> pair = new Pair<>(point, neighbor);
        reachDistances.put(pair, reachDist);
      });
      assert(Tests.reachDistForEachNeighborHasValidValue(point, kNNs.get(point), reachDistances, kDistances, vpKdists, DISTANCE_MEASURE));
    } catch (Exception e) {
      System.out.println("getRds " + e + " " + e.getStackTrace()[0].getLineNumber());
    }
  }

  public static boolean isNeighborOf(Point query, Point center) {
    for (Pair<Point, Double> pair : kNNs.get(center)) {
      if (pair.getValue0().equals(query)) {
        return true;
      }
    }
    return false;
  }

  // This function simply returns all points that contain the query point in their neighborhood
  // while computeRkNNAndUpdateTheirkNNs actually performs the calculation corresponding to this
  // This function assumes the kNNs are valid
  public static HashSet<Point> getRkNN(Point point) {
    HashSet<Point> rknn = new HashSet<>();
    try {
      pointStore.forEach(otherPoint -> {
        if (isNeighborOf(point, otherPoint)) {
          rknn.add(otherPoint);
        }
      });
    } catch (Exception e) {
      System.out.println("getRkNN " + e + " " + e.getStackTrace()[0].getLineNumber());
    }
    return rknn;
  }

  private static boolean allNeighsOnPerim(PriorityQueue<Pair<Point, Double>> knn, Double oldkDist) {
    for (Pair<Point, Double> pair : knn) {
      if (Double.compare(pair.getValue1(), oldkDist) != 0) {
        return false;
      }
    }
    return true;
  }

  // This function returns the kNN after kicking out the farthest neighbors
  // to make space for a new point, resulting in a new kNN with a smaller area
  public static PriorityQueue<Pair<Point, Double>> ejectFarthest(PriorityQueue<Pair<Point, Double>> knn, Double oldkDist) {
    // One edge case I noticed while testing was when all former neighbors are on the perimeter
    // i.e. >= k neighbors are on the perimeter
    // if you were to eject them all, you would end up with an empty kNN (before inserting the new point)
    // which violates the definition of kNN
    // in that case, keep them all on the perimeter, even though new point is strictly inside
    if (allNeighsOnPerim(knn, oldkDist)) return knn;
    while (knn.peek().getValue1().equals(oldkDist)) {
      knn.poll();
    }
    return knn;
  }

  // This function accounts for the change in kNNs that happens as a result of insertion of a new point
  // For each point in the pointStore, it determines its distance to the new point
  // if the distance is smaller than the older point's k-distance, that means the new point has joined its kNN
  // so update the older point's kNN accordingly, while adding the older point to the new point's RkNN
  // The new point may fall right on the old point's kNN's perimeter (if distance == kdistance), so it simply joins the kNN with no further change
  // Or might may fall strictly within the old point's kNN, so in addition to joining the kNN, it ejects at least one former neighbor from it
  // The ejected neighbors are all those on the former perimeter
  // but we must do that while respecting the minimum size of kNN being k
  // so only eject if the current kNN is bigger than k (accounts for beginning of stream)
  public static HashSet<Point> computeRkNNAndUpdateTheirkNNs(Point point) {
    HashSet<Point> rknn = new HashSet<>();
    try {
      pointStore.forEach(x -> {
        if (x.equals(point)) return;
        Double dist;
        if (symDistances.containsKey(new HashSet<Point>(Arrays.asList(point, x)))) {
          dist = symDistances.get(new HashSet<Point>(Arrays.asList(point, x)));
        } else {
          dist = point.getDistanceTo(x, DISTANCE_MEASURE);
          symDistances.put(new HashSet<Point>(Arrays.asList(point, x)), dist);
        }
        if (kNNs.get(x).size() < k || Double.compare(dist, kDistances.get(x)) <= 0) { // if kNN isn't big enough already, or if the new point is in kNN
          rknn.add(x);
          if (kNNs.get(x).size() >= k && dist < kDistances.get(x)) {
            // eject neighbors on the old neighborhood perimeter
            // if there's no more space and the new point is not on that perimeter
            kNNs.put(x, ejectFarthest(kNNs.get(x), kDistances.get(x)));
          }
          kNNs.get(x).add(new Pair<>(point, dist));
          kDistances.put(x, kNNs.get(x).peek().getValue1()); // new k-distance is dist
        }
      });
    } catch (Exception e) {
      System.out.println("computeRkNN " + e + " " + e.getStackTrace()[0].getLineNumber());
    }
    return rknn;
  }

  // This function calculates the LRD according to the equation
  public static void getLrd(Point point) {
    try {
      double rdSum = 0;
      Iterator<Pair<Point, Double>> neighbors = kNNs.get(point).iterator();
      while (neighbors.hasNext()) {
        Point neighbor = neighbors.next().getValue0();
        Pair<Point, Point> pair = new Pair<>(point, neighbor);
        rdSum += reachDistances.get(pair);
      }
      LRDs.put(point, rdSum == 0 ? Double.POSITIVE_INFINITY : (kNNs.get(point).size() / rdSum));
    } catch (Exception e) {
      System.out.println("getLrd " + e + e.getStackTrace()[0].getLineNumber());
    }
  }

  // This function calculates the LOF according to the equation
  public static void getLof(Point point) {
    try {
      double lrdSum = 0;
      Iterator<Pair<Point, Double>> neighbors = kNNs.get(point).iterator();
      while (neighbors.hasNext()) {
        Point neighbor = neighbors.next().getValue0();
        double lrd;
        // VPs may have LRDs (but not RDs, so I don't check this in getLrd)
        if (neighbor.getClass().equals(VPoint.class)) {
          lrd = vpLrds.get(((VPoint)neighbor).center);
        } else {
          lrd = LRDs.get(neighbor);
        }
        lrdSum += lrd;
      }
      LOFs.put(point, lrdSum / (LRDs.get(point) * kNNs.get(point).size()));
    } catch (Exception e) {
      System.out.println("getLof " + e + e.getStackTrace()[0].getLineNumber());
    }
  }

  // If the point is in the max heap topOutliers at that point in time, it is considered an outlier (labeled 1)
  public static Integer labelPoint(Point point) {
    return topOutliers.contains(new Pair<Point, Double>(point, LOFs.get(point))) ? 1 : 0;
  }

  // ILOF.main() is not called from Driver, that's why it does the same config and pre-processing.
  // The only way to run main is to run the jar of ILOF
  public static void main(String[] args) {
    // IMPROVE: better handling of defaults.
    // NOTE: must run from working directory rtlofs.
    Dotenv dotenv = Dotenv.load();

    Properties props = new Properties();
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, dotenv.get("KAFKA_APP_ID"));
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, dotenv.get("KAFKA_BROKER"));
    props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
    props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

    StreamsBuilder builder = new StreamsBuilder();
    KStream<String, String> rawData = builder.stream(dotenv.get("SOURCE_TOPIC"));

    KStream<String, Point> data = rawData.flatMapValues(value -> Arrays.asList(Utils.parse(value,
                                                                              " ",
                                                                              Integer.parseInt(dotenv.get("DIMENSIONS")))));

    process(data, dotenv);

    KafkaStreams streams = new KafkaStreams(builder.build(), props);
    streams.start();

    // IMPROVE: you could add the shutdown hook here too.
  }

  public static void setup(Dotenv config) {
    pointStore = new HashSet<>();
    symDistances = new HashMap<>();
    kNNs = new HashMap<>();
    kDistances = new HashMap<>();
    reachDistances = new HashMap<>();
    LRDs = new HashMap<>();
    LOFs = new HashMap<>();
    // TODO: if these values don't exist in .env, parseInt fails -- check existence first, then try to read, then apply default if value is empty
    // I am not accounting for invalid values, the user should know what they're entering
    k = Optional.ofNullable(Integer.parseInt(config.get("k"))).orElse(3);
    TOP_N = Optional.ofNullable(Integer.parseInt(config.get("TOP_N_OUTLIERS"))).orElse(10);
    DISTANCE_MEASURE = config.get("DISTANCE_MEASURE");
    // whenever you need a max heap, use comparator#reversed() assuming the comparator preserves natural ordering
    topOutliers = MinMaxPriorityQueue.orderedBy(Comparators.pointComparator().reversed()).maximumSize(TOP_N).create();
    totalPoints = 0;
    NNS_TECHNIQUE = config.get("ANNS");
    d = Integer.parseInt(config.get("DIMENSIONS"));
    HASHES = Integer.parseInt(config.get("HASHES"));
    HASHTABLES = Integer.parseInt(config.get("HASHTABLES"));
    HYPERPLANES = Integer.parseInt(config.get("HYPERPLANES"));
    // Along each axis, there are 2 virtual points at each end of the hypersphere bounding the blackhole
    V = 2 * d;
    SINK = Utils.buildSinkFilename(config, false);
  }

  // IMPROVE: this is a pretty nasty function signature
  // This is probably my second least favorite part of this code base.
  public static void ilofSubroutineForRlof(Point point,
                                          HashSet<Point> window,
                                          HashMap<HashSet<Point>, Double> rlofSymDistances,
                                          HashMap<Point, PriorityQueue<Pair<Point, Double>>> rlofkNNs,
                                          HashMap<Point, Double> rlofkDistances,
                                          HashMap<Pair<Point, Point>, Double> rlofreachDistances,
                                          HashMap<Point, Double> rlofLRDs,
                                          HashMap<Point, Double> rlofLOFs,
                                          HashSet<Triplet<Point, Double, Integer>> rlofBlackHoles,
                                          HashMap<Point, Double> rlofVpKdists,
                                          HashMap<Point, Double> rlofVpLrds,
                                          Dotenv config) {
    // BAD! setup executes on each iteration of RLOF
    // which leads some collections to be erased and re-instantiated even when it doesn't make sense
    // IMPROVE: only run ILOF.setup() once
    setup(config);
    // NOTE: cannot import collections from RLOF because otherwise, circular dependency
    // that's why I'm making aliases here
    pointStore = window;
    symDistances = rlofSymDistances;
    kNNs = rlofkNNs;
    kDistances = rlofkDistances;
    reachDistances = rlofreachDistances;
    LRDs = rlofLRDs;
    LOFs = rlofLOFs;
    blackHoles = rlofBlackHoles;
    vpKdists = rlofVpKdists;
    vpLrds = rlofVpLrds;

    computeProfileAndMaintainWindow(point);
  }

  // The core of ILOF logic is all right here.
  // I tried to follow te pseudocode from the ILOF paper as much as possible
  // except when I felt modification was absolutely necessary for correctness
  public static void computeProfileAndMaintainWindow(Point point) {
    try {
      getkNN(point, NNS_TECHNIQUE); // first, calculate new point's kNN
      getRds(point); // then calculate all RDs for each pair (point, neighbor)
      HashSet<Point> update_kdist = computeRkNNAndUpdateTheirkNNs(point); // then maintain for disruption in other kNNs with updating logic
                                                                          // while also returning RkNN of new point, which is the first set of points
                                                                          // to maintain
      assert(Tests.noVirtualPointsAreToBeUpdated(update_kdist));
      // at this stage, points in update_kdist just got their kNNs and k-distances maintained
      HashSet<Point> update_lrd = new HashSet<>(update_kdist);
      for (Point to_update : update_kdist) {
        for (Pair<Point, Double> n : kNNs.get(to_update)) {
          Point neigh = n.getValue0();
          // Because kdistance of points in update_kdist may have changed, update RDs that depend on those kdistances
          // which are RD of neighbors of those points with respect to those points
          if (!(neigh.getClass().equals(VPoint.class))) {
            reachDistances.put(new Pair<>(neigh, to_update), kDistances.get(to_update));
          }
          // NOTE: following not from ILOF paper, but without it, reach_dist(old, new) wouldn't exist.
          Double kdist;
          if (neigh.getClass().equals(VPoint.class)) {
            assert(Tests.isPositive(vpKdists.size()));
            kdist = vpKdists.get(((VPoint)neigh).center);
          } else {
            kdist = kDistances.get(neigh);
          }
          Double dist;
          if (symDistances.containsKey(new HashSet<Point>(Arrays.asList(to_update, neigh)))) {
            dist = symDistances.get(new HashSet<Point>(Arrays.asList(to_update, neigh)));
          } else {
            dist = to_update.getDistanceTo(neigh, DISTANCE_MEASURE);
            symDistances.put(new HashSet<Point>(Arrays.asList(to_update, neigh)), dist);
          }
          // At some point, this neigh is the new point
          reachDistances.put(new Pair<>(to_update, neigh),
                            Math.max(dist, kdist)
                            );
          
          // Beyond this point, we're looking at updating the existing LRD of older points
          // which doesn't apply to the new point nor to virtual points (their LRDs are updated differently, in RLOF.java)
          if (neigh.equals(point) || neigh.getClass().equals(VPoint.class)) {
            continue;
          }
          // NOTE: in ILOF paper, this statement is conditional (if to_update is neighbor of neigh).
          update_lrd.add(neigh);
          // NOTE: following is not from paper either but from notes.
          for (Pair<Point,Double> y : kNNs.get(neigh)) {
            if (y.getValue0().getClass().equals(VPoint.class)) continue;
            update_lrd.add(y.getValue0());
          }
        }
      }
      assert(Tests.noVirtualPointsAreToBeUpdated(update_lrd));
      HashSet<Point> update_lof = new HashSet<>(update_lrd);
      for (Point to_update : update_lrd) {
        getLrd(to_update);
        // after updating LRD (by simply recomputing it according to definition),
        // prepare points whose LOF need updating, and these are the reverse neighbors of points that were just updated
        // again, all the details of updating logic are in the ILOF paper
        update_lof.addAll(getRkNN(to_update));
      }
      // NOTE: in ILOF paper, this was right before getLof(), but getLof(to_update) needs lrd(new).
      getLrd(point);
      assert(Tests.noVirtualPointsAreToBeUpdated(update_lof));
      for (Point to_update : update_lof) {
        if (to_update.equals(point)) continue;
        getLof(to_update);
      }
      getLof(point);
    } catch (Exception e) {
      System.out.println("computeProfileAndMaintainWindow " + e);
      e.printStackTrace();
    }
  }

  public static void process(KStream<String, Point> data, Dotenv config) {
    setup(config);
    data.flatMap((key, point) -> {
      // This key, read from the kafka topic, is used for printing the labels to the sink files
      point.setKey(key);
      pointStore.add(point);
      totalPoints++;
      // only start counting time elapsed when processing has started
      // because kafka streams may take some time before it starts injesting the source topic, even if the topic is nonempty
      if (totalPoints == 1) {
        startTime = System.nanoTime();
      }
      computeProfileAndMaintainWindow(point); // bulk of logic
      // mapped contains the label for each point, uniquely identified by its key
      ArrayList<KeyValue<String, Integer>> mapped = new ArrayList<>();
      if (totalPoints == Integer.parseInt(config.get("TOTAL_POINTS"))) { // if we're done with the dataset
        long estimatedEndTime = System.nanoTime();
        for (Point x : pointStore) {
          // could have also done the following after computeProfileAndMaintainWindow call
          topOutliers.add(new Pair<>(x, LOFs.get(x)));
        };
        for (Point x : pointStore) {
          // IMPROVE: impl verbose mode, also in .env
          // this printing to stdout is useful for debugging smaller datasets, like dummy
          System.out.println(x);
          System.out.println(kNNs.get(x));
          System.out.println(kDistances.get(x));
          for (Pair<Point,Double> p : kNNs.get(x)) {
            System.out.print(reachDistances.get(new Pair<>(x, p.getValue0())) + " ");
          }
          System.out.println("\n" + LRDs.get(x));
          System.out.println(LOFs.get(x));
          // System.out.println("label " + labelPoint(x));
          // System.out.println(x.key + " " + labelPoint(x));
          mapped.add(new KeyValue<String, Integer>(x.key, labelPoint(x)));
        };
        System.out.println("Estimated time elapsed ms " + (estimatedEndTime - startTime) / 1000000); // nanosecond -> ms
      }
      return mapped;
    })
    // only prints once, in ILOF, at the end of the stream
    // NOTE: not a big fan of the default format of the output, but I haven't really experimented
    .print(Printed.toFile(SINK));

    // IMPROVE: write to sink topic
    // final Serde<String> stringSerde = Serdes.String();
    // <some_stream>.toStream().to("some-topic", Produced.with(stringSerde, stringSerde));

  }
}
