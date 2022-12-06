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

public class ILOF {

  // IMPROVE: Conditionally initiliaze these collections depeending on
  // whether ILOF is called from Driver or from another algorithm.
  public static HashSet<Point> pointStore;
  public static HashMap<HashSet<Point>, Double> symDistances;
  public static HashMap<Point, PriorityQueue<Pair<Point, Double>>> kNNs;
  public static HashMap<Point, Double> kDistances;
  public static HashMap<Pair<Point, Point>, Double> reachDistances;
  public static HashMap<Point, Double> LRDs;
  public static HashMap<Point, Double> LOFs;
  public static HashSet<Triplet<Point, Double, Integer>> blackHoles;
  public static HashMap<Point, Double> vpKdists;
  public static HashMap<Point, Double> vpLrds;

  public static int k;
  public static int d;
  public static int TOP_N;
  public static String DISTANCE_MEASURE;
  public static String NNS_TECHNIQUE;
  public static int HASHES;
  public static int HASHTABLES;
  public static int V;
  public static String SINK;

  public static MinMaxPriorityQueue<Pair<Point, Double>> topOutliers;
  public static long totalPoints;
  public static long startTime;

  public static void getTarsosLshkNN(Point point) {
    HashFamily hashFamily = null;
    List<Vector> dataset = Sets.difference(pointStore, new HashSet<Point>(Arrays.asList(point))).stream().map(Point::toVector).collect(Collectors.toList());
    assert(Tests.pointNotInDataset(point, dataset));
    if (blackHoles != null && blackHoles.size() > 0) {
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
      // IMPROVE: implement proper logging everywhere in this project + error log.
      default: System.out.println("Unsupported distance measure.");
    }

    // Neighbors could be Points or VPoints.
    List<Point> neighbors = CommandLineInterface.lshSearch(dataset,
                            hashFamily,
                            HASHES,
                            HASHTABLES,
                            Arrays.asList(point.toVector()),
                            k)
                            .get(0)
                            .stream()
                            .map(Point::fromVector)
                            .collect(Collectors.toList());

    assert(Tests.pointNotInNeighbors(point, neighbors));

    // IMPROVE: try to guarantee a minimum number of neighbors from Tarsos.

    PriorityQueue<Pair<Point, Double>> pq = new PriorityQueue<>(PointComparator.comparator().reversed());
    for (Point n : neighbors) {
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

  public static ArrayList<VPoint> deriveVirtualPoints() {
    ArrayList<VPoint> ans = new ArrayList<>();
    try {
      blackHoles.forEach(bh -> {
        for (int pl = 0; pl < d; pl++) {
          for (int pos = 0; pos < 2; pos++) {
            ans.add(new VPoint(bh.getValue0(), bh.getValue1(), d, pl, pos));
          }
        }
      });
    } catch (Exception e) {
      System.out.println("deriveVirtualPoints " + e + " " + e.getStackTrace()[0].getLineNumber());
    }
    assert(Tests.isEq(ans.size(), blackHoles.size() * d * 2));
    return ans;
  }

  public static void getFlatkNN(Point point) {
    try {
      ArrayList<Pair<Point, Double>> distances = new ArrayList<>();
      pointStore.forEach(otherPoint -> {
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
      if (blackHoles != null && blackHoles.size() > 0) {
        ArrayList<VPoint> vps = deriveVirtualPoints();
        vps.forEach(vp -> {
          Double dist;
          if (symDistances.containsKey(new HashSet<Point>(Arrays.asList(point, vp)))) {
            dist = symDistances.get(new HashSet<Point>(Arrays.asList(point, vp)));
          } else {
            dist = point.getDistanceTo(vp, DISTANCE_MEASURE);
            symDistances.put(new HashSet<Point>(Arrays.asList(point, vp)), dist);
          }
          distances.add(new Pair<Point, Double>(vp, dist));
        });
      }
      assert(Tests.isEq(distances.size(), pointStore.size() - 1  + (blackHoles != null ? blackHoles.size() * 2 * d : 0)));
      distances.sort(PointComparator.comparator());
      if (distances.size() >= 2) {
        assert(Tests.isSortedAscending(distances));
      }
      Double kdist = 0.0;
      if (distances.size() > 0) {
        kdist = distances.get(Math.min(k-1, distances.size()-1)).getValue1();
      }
      kDistances.put(point, kdist == 0 ? Double.POSITIVE_INFINITY : kdist);
      int i = k;
      for (; i < distances.size() && distances.get(i).getValue1().equals(kdist); i++) { }
      PriorityQueue<Pair<Point, Double>> pq = new PriorityQueue<>(PointComparator.comparator().reversed());
      if (distances.size() > 0) {
        pq.addAll(distances.subList(0, Math.min(i, distances.size())));
      }
      assert(Tests.isMaxHeap(new PriorityQueue<Pair<Point, Double>>(pq)));
      kNNs.put(point, pq);
      if (totalPoints > k) {
        assert(Tests.atLeastKNeighbors(kNNs.get(point), k));
      }
    } catch (Exception e) {
      System.out.println("getFlatkNN " + e + " " + e.getStackTrace()[0].getLineNumber());
    }
  }

  public static void getkNN(Point point, String NNS_TECHNIQUE) {
    switch (NNS_TECHNIQUE) {
      case "FLAT": getFlatkNN(point); return;
      case "LSH": getTarsosLshkNN(point); return;
      default: System.out.println("Unsupported nearest neighbor search technique.");
    }
  }

  public static void getRds(Point point) {
    try {
      kNNs.get(point).forEach(neighborpair -> {
        Point neighbor = neighborpair.getValue0();
        Double kdist;
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

  public static PriorityQueue<Pair<Point, Double>> ejectFarthest(PriorityQueue<Pair<Point, Double>> knn, Double oldkDist) {
    if (allNeighsOnPerim(knn, oldkDist)) return knn;
    while (knn.peek().getValue1().equals(oldkDist)) {
      knn.poll();
    }
    return knn;
  }

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
        if (kNNs.get(x).size() < k || Double.compare(dist, kDistances.get(x)) <= 0) {
          rknn.add(x);
          if (kNNs.get(x).size() >= k && dist < kDistances.get(x)) {
            // eject neighbors on the old neighborhood perimeter
            // if there's no more space and the new point is not on that perimeter
            kNNs.put(x, ejectFarthest(kNNs.get(x), kDistances.get(x)));
          }
          kNNs.get(x).add(new Pair<>(point, dist));
          kDistances.put(x, kNNs.get(x).peek().getValue1());
        }
      });
    } catch (Exception e) {
      System.out.println("computeRkNN " + e + " " + e.getStackTrace()[0].getLineNumber());
    }
    return rknn;
  }

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

  public static void getLof(Point point) {
    try {
      double lrdSum = 0;
      Iterator<Pair<Point, Double>> neighbors = kNNs.get(point).iterator();
      while (neighbors.hasNext()) {
        Point neighbor = neighbors.next().getValue0();
        double lrd;
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

  public static Integer labelPoint(Point point) {
    return topOutliers.contains(new Pair<Point, Double>(point, LOFs.get(point))) ? 1 : 0;
  }

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
  }

  public static void setup(Dotenv config) {
    pointStore = new HashSet<>();
    symDistances = new HashMap<>();
    kNNs = new HashMap<>();
    kDistances = new HashMap<>();
    reachDistances = new HashMap<>();
    LRDs = new HashMap<>();
    LOFs = new HashMap<>();
    // BUG: if these values don't exist in .env, parseInt fails.
    k = Optional.ofNullable(Integer.parseInt(config.get("k"))).orElse(3);
    TOP_N = Optional.ofNullable(Integer.parseInt(config.get("TOP_N_OUTLIERS"))).orElse(10);
    DISTANCE_MEASURE = config.get("DISTANCE_MEASURE");
    topOutliers = MinMaxPriorityQueue.orderedBy(PointComparator.comparator().reversed()).maximumSize(TOP_N).create();
    totalPoints = 0;
    NNS_TECHNIQUE = config.get("ANNS");
    d = Integer.parseInt(config.get("DIMENSIONS"));
    HASHES = Integer.parseInt(config.get("HASHES"));
    HASHTABLES = Integer.parseInt(config.get("HASHTABLES"));
    // Along each axis, there are 2 virtual points at each end of the hypersphere bounding the blackhole
    V = 2 * d;
    SINK = Utils.buildSinkFilename(config, false);
  }

  // IMPROVE: this is a pretty nasty function signature
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
    // IMPROVE: There's some overlap between RLOF.setup() and ILOF.setup()
    setup(config);
    // NOTE: cannot import collections from RLOF because otherwise, circular dependency

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

  public static void computeProfileAndMaintainWindow(Point point) {
    try {
      getkNN(point, NNS_TECHNIQUE);
      getRds(point);
      HashSet<Point> update_kdist = computeRkNNAndUpdateTheirkNNs(point);
      assert(Tests.noVirtualPointsAreToBeUpdated(update_kdist));
      HashSet<Point> update_lrd = new HashSet<>(update_kdist);
      for (Point to_update : update_kdist) {
        for (Pair<Point, Double> n : kNNs.get(to_update)) {
          Point neigh = n.getValue0();
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
          reachDistances.put(new Pair<>(to_update, neigh),
                            Math.max(dist, kdist)
                            );
          
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
      point.setKey(key);
      pointStore.add(point);
      totalPoints++;
      if (totalPoints == 1) {
        startTime = System.nanoTime();
    }
      computeProfileAndMaintainWindow(point);
      ArrayList<KeyValue<String, Integer>> mapped = new ArrayList<>();
      if (totalPoints == Integer.parseInt(config.get("TOTAL_POINTS"))) {
        long estimatedEndTime = System.nanoTime();
        for (Point x : pointStore) {
          topOutliers.add(new Pair<>(x, LOFs.get(x)));
        };
        for (Point x : pointStore) {
          // IMPROVE: impl verbose mode everywhere
          
          // System.out.println(x);
          // System.out.println(kNNs.get(x));
          // System.out.println(kDistances.get(x));
          // for (Pair<Point,Double> p : kNNs.get(x)) {
          //   System.out.print(reachDistances.get(new Pair<>(x, p.getValue0())) + " ");
          // }
          // System.out.println(LRDs.get(x));
          // System.out.println(LOFs.get(x));
          // System.out.println("label " + labelPoint(x));
          // System.out.println(x.key + " " + labelPoint(x));
          mapped.add(new KeyValue<String, Integer>(x.key, labelPoint(x)));
        };
        System.out.println("Estimated time elapsed ms " + (estimatedEndTime - startTime) / 1000000);
      }
      return mapped;
    })
    .print(Printed.toFile(SINK));

    // IMPROVE: write to sink topic
    // final Serde<String> stringSerde = Serdes.String();
    // <some_stream>.toStream().to("some-topic", Produced.with(stringSerde, stringSerde));

  }
}
