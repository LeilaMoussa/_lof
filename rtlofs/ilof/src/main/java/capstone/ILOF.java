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

  // These collections should only be initialized and used when standalone ILOF is run.
  public static HashSet<Point> pointStore;
  // TODO: I'm not using the pq logic anymore!
  public static HashMap<Point, PriorityQueue<Pair<Point, Double>>> kNNs;
  public static HashMap<Point, Double> kDistances;
  public static HashMap<Pair<Point, Point>, Double> reachDistances;
  public static HashMap<Point, Double> LRDs;
  public static HashMap<Point, Double> LOFs;
  // the following not allocated unless in RLOF
  public static HashSet<Triplet<Point, Double, Integer>> blackHoles;
  public static HashMap<Point, Double> vpKdists;
  public static HashMap<Point, Double> vpLrds;

  // NOTE: not all these variables are relevant all the time.
  // Conditional initialization? Eh...
  public static int k;
  public static int d;
  public static int TOP_N;
  public static String DISTANCE_MEASURE;
  public static String NNS_TECHNIQUE;
  public static int HASHES;
  public static int HASHTABLES;
  public static int V;

  public static MinMaxPriorityQueue<Pair<Point, Double>> topOutliers;
  public static long totalPoints;

  public static long startTime;

  public static void getTarsosLshkNN(Point point) {
    HashFamily hashFamily = null;
    List<Vector> dataset = Sets.difference(pointStore, new HashSet<Point>(Arrays.asList(point))).stream().map(Point::toVector).collect(Collectors.toList());
    // assert dataset doesn't contain point
    dataset.addAll(deriveVirtualPoints().stream().map(Point::toVector).collect(Collectors.toList()));
    // assert all vectors with non null keys in dataset sum up to blackholes * 2 * d
    switch (DISTANCE_MEASURE) {
      case "EUCLIDEAN":
        int radiusEuclidean = (int)Math.ceil(LSH.determineRadius(dataset, new EuclideanDistance(), 40));
        // assert if the dataset is non empty, the radius is positive, here and below
        hashFamily = new EuclidianHashFamily(radiusEuclidean, d);
        break;
      case "MANHATTAN":
        //int radiusCityBlock = (int) LSH.determineRadius(dataset, new CityBlockDistance(), 20);
        int radiusCityBlock = (int)Math.ceil(LSH.determineRadius(dataset, new CityBlockDistance(), 40));
        hashFamily = new CityBlockHashFamily(radiusCityBlock, d);
        break;
      default: System.out.println("Unsupported distance measure.");
    }

    List<Point> neighbors = CommandLineInterface.lshSearch(dataset,
                            hashFamily,
                            HASHES,
                            HASHTABLES,
                            Arrays.asList(point.toVector()),
                            k)
                            .get(0) // the first elt a list that's always guaranteed to be there but may be empty
                            .stream()
                            .map(Point::fromVector) // could be VPoint
                            .collect(Collectors.toList());

    // assert neighbors does not contain point

    // later: assert there are enough neighbors, maybe >= k-1 ?
    // if totalPoints > 0, assert neighbors are more than 0

    PriorityQueue<Pair<Point, Double>> pq = new PriorityQueue<>(PointComparator.comparator().reversed());
    for (Point n : neighbors) {
      //if (n.getClass().equals(VPoint.class))
      pq.add(new Pair<Point, Double>(n, point.getDistanceTo(n, DISTANCE_MEASURE)));
    }
    // assert pq is max heap
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
        Double distance = point.getDistanceTo(otherPoint, DISTANCE_MEASURE);
        distances.add(new Pair<Point, Double>(otherPoint, distance));
      });
      if (blackHoles != null) {
        ArrayList<VPoint> vps = deriveVirtualPoints();
        vps.forEach(vp -> {
          Double distance = point.getDistanceTo(vp, DISTANCE_MEASURE);
          distances.add(new Pair<Point, Double>(vp, distance));
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
        double reachDist = Math.max(kdist, 
                                    point.getDistanceTo(neighbor, DISTANCE_MEASURE));
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

  public static HashSet<Point> computeRkNN(Point point) {
    HashSet<Point> rknn = new HashSet<>();
    try {
      pointStore.forEach(x -> {
        if (x.equals(point)) return;
        Double dist = point.getDistanceTo(x, DISTANCE_MEASURE);
        if (kNNs.get(x).size() < k || dist <= kDistances.get(x)) {
          rknn.add(x);
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
    // TODO: better handling of defaults.
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
    kNNs = new HashMap<>();
    kDistances = new HashMap<>();
    reachDistances = new HashMap<>();
    LRDs = new HashMap<>();
    LOFs = new HashMap<>();
    // BUG if these values don't exist in .env, parseInt fails.
    k = Optional.ofNullable(Integer.parseInt(config.get("k"))).orElse(3);
    TOP_N = Optional.ofNullable(Integer.parseInt(config.get("TOP_N_OUTLIERS"))).orElse(10);
    DISTANCE_MEASURE = config.get("DISTANCE_MEASURE");
    topOutliers = MinMaxPriorityQueue.orderedBy(PointComparator.comparator().reversed()).maximumSize(TOP_N).create();
    totalPoints = 0;
    NNS_TECHNIQUE = config.get("ANNS");
    d = Integer.parseInt(config.get("DIMENSIONS"));
    HASHES = Integer.parseInt(config.get("HASHES"));
    HASHTABLES = Integer.parseInt(config.get("HASHTABLES"));
    // Along each axis, there are 2 virtual points at each end of the hypersphere bounding the blackhole.
    V = 2 * d;
  }

  // this is a pretty nasty function signature
  public static void ilofSubroutineForRlof(Point point,
                                          HashSet<Point> window,
                                          HashMap<Point, PriorityQueue<Pair<Point, Double>>> rlofkNNs,
                                          HashMap<Point, Double> rlofkDistances,
                                          HashMap<Pair<Point, Point>, Double> rlofreachDistances,
                                          HashMap<Point, Double> rlofLRDs,
                                          HashMap<Point, Double> rlofLOFs,
                                          HashSet<Triplet<Point, Double, Integer>> rlofBlackHoles,
                                          HashMap<Point, Double> rlofVpKdists,
                                          HashMap<Point, Double> rlofVpLrds,
                                          Dotenv config) {
    // TODO: There's some overlap between RLOF.setup() and ILOF.setup()
    setup(config);
    // reminder to self: i did this to avoid circular dependency

    pointStore = window;
    kNNs = rlofkNNs;
    kDistances = rlofkDistances;
    reachDistances = rlofreachDistances;
    LRDs = rlofLRDs;
    LOFs = rlofLOFs;
    blackHoles = new HashSet<>(rlofBlackHoles);
    vpKdists = new HashMap<>(rlofVpKdists);
    vpLrds = new HashMap<>(rlofVpLrds);

    computeProfileAndMaintainWindow(point);
  }

  public static void computeProfileAndMaintainWindow(Point point) {
    try {
      getkNN(point, NNS_TECHNIQUE);
      getRds(point);
      HashSet<Point> update_kdist = computeRkNN(point);
      assert(Tests.noVirtualPointsAreToBeUpdated(update_kdist));
      for (Point to_update : update_kdist) {
        // TODO: i could write updatekDist() that performs the update logic from querykNN()
        // for slightly better performance => i should do this (i.e. push and pop logic)
        // NOTE: that would only be useful for flatsearch
        getkNN(to_update, NNS_TECHNIQUE);
      }
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
            // TODO check this kdist, check that VPs returned from vectors are good
            // check that VPoint equals is good and doesn't rely on hashcode, but probably modify hashcode to be sure
            kdist = vpKdists.get(((VPoint)neigh).center);
          } else {
            kdist = kDistances.get(neigh);
          }
          reachDistances.put(new Pair<>(to_update, neigh),
                            Math.max(
                              to_update.getDistanceTo(neigh, DISTANCE_MEASURE),
                              kdist)
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
      System.out.println("computeProfileAndMaintainWindow " + e + " " + e.getStackTrace()[0].getLineNumber());
    }
  }

  public static void process(KStream<String, Point> data, Dotenv config) {
    setup(config);
    data.flatMap((key, point) -> {
      pointStore.add(point);
      totalPoints++;
      if (totalPoints == 1) {
        startTime = System.nanoTime();
    }
      System.out.println("" + totalPoints);
      computeProfileAndMaintainWindow(point);
      ArrayList<KeyValue<String, Integer>> mapped = new ArrayList<>();
      if (totalPoints == Integer.parseInt(config.get("TOTAL_POINTS"))) {
        long estimatedEndTime = System.nanoTime();
        System.out.println("estimated time elapsed ms " + (estimatedEndTime - startTime) / 1000000);
        for (Point x : pointStore) {
          topOutliers.add(new Pair<>(x, LOFs.get(x)));
        };
        for (Point x : pointStore) {
          // System.out.println(x);
          // System.out.println(kNNs.get(x));
          // System.out.println(kDistances.get(x));
          // for (Pair<Point,Double> p : kNNs.get(x)) {
          //   System.out.print(reachDistances.get(new Pair<>(x, p.getValue0())) + " ");
          // }
          // System.out.println(LRDs.get(x));
          // System.out.println(LOFs.get(x));
          // System.out.println("label " + labelPoint(x));
          System.out.println(x + "" + labelPoint(x));
          mapped.add(new KeyValue<String, Integer>(x.toString(), labelPoint(x)));
        };
      }
      return mapped;
    })
    // TODO: I don't like this format
    // if i can't change the format from here, just gonna have to do it myself in roc.py
    .print(Printed.toFile(Utils.buildSinkFilename(config, false, false)));

    // final Serde<String> stringSerde = Serdes.String();
    // <some_stream>.toStream().to("some-topic", Produced.with(stringSerde, stringSerde));

  }
}
