package capstone;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Printed;
import com.google.common.collect.MinMaxPriorityQueue;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.PriorityQueue;
import java.util.Properties;
import java.util.Set;
import java.util.ArrayList;
import java.util.Optional;

import org.javatuples.Pair;
import io.github.cdimascio.dotenv.Dotenv;

public class ILOF {

  public static HashMap<Point, Point> pointStore;
  public static HashMap<Set<Point>, Double> symDistances;
  public static HashMap<Point, PriorityQueue<Pair<Point, Double>>> kNNs;
  public static HashMap<Point, Double> kDistances;
  public static HashMap<Pair<Point, Point>, Double> reachDistances;
  public static HashMap<Point, Integer> neighborhoodCardinalities;
  public static HashMap<Point, Double> LRDs;
  public static HashMap<Point, Double> LOFs;
  public static HashMap<Point, HashSet<Point>> RkNNs;

  public static HashSet<Point> kDistChanged;
  public static HashSet<Point> reachDistChanged;
  public static HashSet<Point> neighCardinalityChanged;
  public static HashSet<Point> lrdChanged;
  public static int k;
  public static int topN;
  public static int topPercent;
  
  public static MinMaxPriorityQueue<Pair<Point, Double>> topOutliers;
  public static int totalPoints;

  public static KeyValue<Point, Point> calculateSymmetricDistances(Point point, String distanceMeasure) {
    try {
      // TODO: implement proper logging.
      System.out.println(point);
      totalPoints += 1;
      pointStore.values().forEach((otherPoint) -> {
        final double distance = point.getDistanceTo(otherPoint, distanceMeasure);
        symDistances.put(new HashSet<Point>(Arrays.asList(point, otherPoint)), distance);
      });
      pointStore.put(point, point);
    } catch (Exception e) {
      System.out.println("1 " + e);
    }
    return new KeyValue<Point, Point>(point, point);
  }

  public static KeyValue<Point, Point> querykNN(Point point) {
    try {
      ArrayList<Pair<Point, Double>> distances = new ArrayList<>();
      pointStore.values().forEach(otherPoint -> {
        if (otherPoint.equals(point)) return;
        double distance = symDistances.get(new HashSet<Point>(Arrays.asList(point, otherPoint)));
        distances.add(new Pair<Point, Double>(otherPoint, distance));
        // update kNNs of past points to be able to get correct RkNN later
        if (kNNs.containsKey(otherPoint) && kDistances.containsKey(otherPoint)) {
          boolean cardChanged = false;
          if (distance < kDistances.get(otherPoint)) {
            while (kNNs.get(otherPoint).isEmpty() == false && kNNs.get(otherPoint).peek().getValue1() == kDistances.get(otherPoint)) {
              kNNs.get(otherPoint).poll();
            }
            kNNs.get(otherPoint).add(new Pair<Point, Double>(point, distance));
            kDistances.put(otherPoint, distance);
            kDistChanged.add(otherPoint);
            cardChanged = true;
          } else if (distance == kDistances.get(otherPoint)) {
            kNNs.get(otherPoint).add(new Pair<Point,Double>(point, distance));
            cardChanged = true;
          }
          neighborhoodCardinalities.put(otherPoint, kNNs.get(otherPoint).size());
          if (cardChanged) {
            neighCardinalityChanged.add(otherPoint);
            // by now, i'm expecting rd(otherPoint, x) to exist for all its neighbors
            for (Pair<Point,Double> pair : kNNs.get(otherPoint)) {
              Pair<Point, Point> x = new Pair<>(otherPoint, pair.getValue0());
              if (reachDistances.containsKey(x) == false) {
                System.out.println("bad point " + otherPoint);
                // first point is expected to be bad when second point comes around
                // because its knn is an empty set
              }
            }
          }
        }
      });
      distances.sort(PointComparator.comparator());
      double kdist = 0;
      if (distances.size() > 0) {
        kdist = distances.get(Math.min(k-1, distances.size()-1)).getValue1();
      }
      kDistances.put(point, kdist == 0 ? Double.POSITIVE_INFINITY : kdist);
      int i = k;
      for (; i < distances.size() && distances.get(i).getValue1() == kDistances.get(point); i++) { }
      PriorityQueue<Pair<Point, Double>> pq = new PriorityQueue<>(PointComparator.comparator().reversed());
      if (distances.size() > 0) {
        distances.subList(0, Math.min(i, distances.size()-1)).forEach(neighbor -> {
          //if (neighbor.getValue0().equals(point)) return;
          pq.add(neighbor);
        });
      }
      kNNs.put(point, pq);
      neighborhoodCardinalities.put(point, pq.size());
    } catch (Exception e) {
      System.out.println("2 " + e);
    }
    return new KeyValue<Point, Point>(point, point);
  }

  public static KeyValue<Point, Point> calculateReachDist(Point point) {
    try {
      kNNs.get(point).forEach(neighbor -> {
        // here, i'm only defining reachdist for (a, b) where b is neighbor of a
        // theoretically, it can be defined for any 2 points
        // but i think practically, i only need this for lrd computation
        double reachDist = Math.max(kDistances.get(neighbor.getValue0()), symDistances.get(new HashSet<Point>(Arrays.asList(point, neighbor.getValue0()))));
        Pair<Point, Point> pair = new Pair<>(point, neighbor.getValue0());
        // keep this old vs new check here?
        Double oldRdIfAny = null;
        if (reachDistances.containsKey(pair)) {
          oldRdIfAny = reachDistances.get(pair);
        }
        System.out.println("making rd for " + point);
        reachDistances.put(pair, reachDist);
        if (oldRdIfAny != null && oldRdIfAny != reachDist) {
          reachDistChanged.add(point);
        }
      });
    } catch (Exception e) {
      System.out.println("3 " + e);
    }
    return new KeyValue<Point, Point>(point, point);
  }

  public static KeyValue<Point, Point> calculateLocalReachDensity(Point point, boolean update) {
    try {
      double rdSum = 0;
      Iterator<Pair<Point, Double>> neighbors = kNNs.get(point).iterator();
      while (neighbors.hasNext()) {
        Point neighbor = neighbors.next().getValue0();
        Pair<Point, Point> pair = new Pair<>(point, neighbor);
        rdSum += reachDistances.get(pair); // null exception in update mode, but not for all points
        // not a freaking clue why
      }
      LRDs.put(point, rdSum == 0 ? Double.POSITIVE_INFINITY : neighborhoodCardinalities.get(point) / rdSum);
    } catch (Exception e) {
      System.out.println("4  " + e + " point " + point + "in update mode " + update);
    }
    return new KeyValue<Point, Point>(point, point);
  }

  public static KeyValue<Point, Point> calculateLocalOutlierFactor(Point point) {
    try {
      double lrdSum = 0;
      Iterator<Pair<Point, Double>> neighbors = kNNs.get(point).iterator();
      while (neighbors.hasNext()) {
        lrdSum += LRDs.get(neighbors.next().getValue0());
      }
      LOFs.put(point, lrdSum / (LRDs.get(point) * neighborhoodCardinalities.get(point)));
    } catch (Exception e) {
      System.out.println("5 " + e);
    }
    return new KeyValue<Point, Point>(point, point);
  }

  public static KeyValue<Point, Point> queryReversekNN(Point point) {
    try {
      HashSet<Point> rknn = new HashSet<>();
      pointStore.values().forEach(otherPoint -> {
        if (otherPoint.equals(point)) return;
        double dist = symDistances.get(new HashSet<Point>(Arrays.asList(point, otherPoint)));
        if (kNNs.get(otherPoint).contains(new Pair<Point, Double>(point, dist))) {
          rknn.add(otherPoint);
        }
      });
      RkNNs.put(point, rknn);
    } catch (Exception e) {
      System.out.println("6 " + e);
    }
    return new KeyValue<Point, Point>(point, point);
  }

  public static void calculateReachDistOfPair(Point center, Point fixedOther) {
    // this is where the rd changes, so i will have to check whether fixedOther is a neighbor of center
    try {
      double reachDist = Math.max(kDistances.get(fixedOther), symDistances.get(new HashSet<Point>(Arrays.asList(center, fixedOther))));
      Pair<Point, Point> pair = new Pair<>(center, fixedOther);
      Pair<Point, Double> potentialNeighbor = new Pair<>(fixedOther, symDistances.get(new HashSet<Point>(Arrays.asList(center, fixedOther))));
      if (kNNs.get(center).contains(potentialNeighbor)) {
        // only if we're talking about an rd change wrt a neighbor
        // do we check if the rd changed

        // with this same condition (if neighbor), ilof pseudocode
        // adds center to set whose lrd will change
        Double oldRdIfAny = null;
        if (reachDistances.containsKey(pair)) {
          oldRdIfAny = reachDistances.get(pair);
        }
        if (oldRdIfAny != null && oldRdIfAny != reachDist) {
          reachDistances.put(pair, reachDist);
          reachDistChanged.add(center);
        }
      }
    } catch (Exception e) {
      System.out.println("6.5 " + e);
    }
  }

  public static KeyValue<Point, Point> updateReachDists(Point point) {
    try {
      kDistChanged.forEach(somePoint -> {
        // this updates rd(n1, n2) for n1 in knn(somepoint) where somepoint's kdist changed
        kNNs.get(somePoint).forEach(neighbor -> {
          //calculateReachDist(neighbor.getValue0());  // i need to update rd(neighbor, somePoint) only, not rd(neighbor, neighbor_of_neighbor)
          // ilof pseudocode says all neighbors but new point
          // so let's see if this make sense
          if (neighbor.getValue0().equals(point)) return;
          calculateReachDistOfPair(neighbor.getValue0(), somePoint);
        });
      });
    } catch (Exception e) {
      System.out.println("7 " + e);
    }
    return new KeyValue<Point, Point>(point, point);
  }

  public static KeyValue<Point, Point> updateLocalReachDensities(Point point) {
    try {
      HashSet<Point> target = new HashSet<>();

      target.addAll(neighCardinalityChanged); // the pseudocode suggests this should be replaced with kDistChanged? even that doesn't fix the errors
      
      // the following is wrong :(
      // reachDistChanged.forEach(rdChanged -> {
      //   target.addAll(RkNNs.get(rdChanged));
      // });

      target.addAll(reachDistChanged); // not prob
      target.forEach(toUpdate -> {
        double oldLrd = LRDs.get(toUpdate);
        calculateLocalReachDensity(toUpdate, true);
        if (oldLrd != LRDs.get(toUpdate)) {
          lrdChanged.add(toUpdate);
        }
      });
    } catch (Exception e) {
      System.out.println("8 " + e);
    }
    return new KeyValue<Point, Point>(point, point);
  }

  public static KeyValue<Point, Point> updateLocalOutlierFactors(Point point) {
    try {
      HashSet<Point> target = new HashSet<>();
      target.addAll(lrdChanged);
      // i think this is correct
      lrdChanged.forEach(changed -> {
        target.addAll(RkNNs.get(changed));
      });
      target.forEach(toUpdate -> {
        calculateLocalOutlierFactor(toUpdate);
      });
    } catch (Exception e) {
      System.out.println("9 " + e);
    }
    return new KeyValue<Point, Point>(point, point);
  }

  public static KeyValue<Point, Double> clearDisposableSetsAndReturnCurrentScore(Point point) {
    try {
      kDistChanged.clear();
      reachDistChanged.clear();
      neighCardinalityChanged.clear();
      lrdChanged.clear();
    } catch (Exception e) {
      System.out.println("10 " + e);
    }
    return new KeyValue<Point, Double>(point, LOFs.get(point));
  }

  public static KeyValue<Point, Double> getTopNOutliers(Point point, Double lof) {
    try {
      // add to max heap of fixed size
      topOutliers.add(new Pair<Point, Double>(point, lof));
      if (totalPoints == 500) {
        System.out.println("TOP OUTLIERS");
        while (topOutliers.size() > 0) {
          Pair<Point, Double> max = topOutliers.poll();
          System.out.println(max.getValue0() + " : " + max.getValue1());
        }
      }
    } catch (Exception e) {
      System.out.println("11 " + e);
    }
    return new KeyValue<Point, Double>(point, lof);
  }

  public static void main(String[] args) {
    // TODO: better handling of defaults.
    // NOTE: run from working directory rtlofs.
    Dotenv dotenv = Dotenv.load();

    Properties props = new Properties();
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, dotenv.get("KAFKA_APP_ID"));
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, dotenv.get("KAFKA_BROKER"));
    props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
    props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

    StreamsBuilder builder = new StreamsBuilder();
    KStream<String, String> rawData = builder.stream(dotenv.get("SOURCE_TOPIC"));

    KStream<String, Point> data = rawData.flatMapValues(value -> Arrays.asList(Parser.parse(value,
                                                                              " ",
                                                                              Integer.parseInt(dotenv.get("DIMENSIONS")))));

    processScratch(data, dotenv);

    KafkaStreams streams = new KafkaStreams(builder.build(), props);
    streams.start();
  }

  public static void setup(Dotenv config) {
    pointStore = new HashMap<>();
    symDistances = new HashMap<>();
    kNNs = new HashMap<>();
    kDistances = new HashMap<>();
    reachDistances = new HashMap<>();
    neighborhoodCardinalities = new HashMap<>();
    LRDs = new HashMap<>();
    LOFs = new HashMap<>();
    RkNNs = new HashMap<>();
    kDistChanged = new HashSet<>();
    reachDistChanged = new HashSet<>();
    neighCardinalityChanged = new HashSet<>();
    lrdChanged = new HashSet<>();
    k = Optional.ofNullable(Integer.parseInt(config.get("k"))).orElse(3);
    topN = Optional.ofNullable(Integer.parseInt(config.get("TOP_N_OUTLIERS"))).orElse(10);
    topPercent = Optional.ofNullable(Integer.parseInt(config.get("TOP_PERCENT_OUTLIERS"))).orElse(5);
    topOutliers = MinMaxPriorityQueue.orderedBy(PointComparator.comparator().reversed()).maximumSize(topN).create();
    totalPoints = 0;
  }

  public static void process(KStream<String, Point> data, Dotenv config) {
    // This function contains the standalone ILOF algorithm.

    // One way to make sure each iteration does not get messed up by the point after it
    // is to forward the needed data in the following stage => no global state?
    setup(config);
    KStream<Point, Double> lofScores = 
      data
      .map((key, formattedPoint) -> calculateSymmetricDistances(formattedPoint, 
                                      Optional.ofNullable(config.get("DISTANCE_MEASURE"))
                                      .orElse("EUCLIDEAN")))
      .map((key, point) -> querykNN(point))
      .map((key, point) -> calculateReachDist(point))
      .map((key, point) -> calculateLocalReachDensity(point, false))
      .map((key, point) -> calculateLocalOutlierFactor(point))

      .map((key, point) -> queryReversekNN(point))
      .map((key, point) -> updateReachDists(point))
      .map((key, point) -> updateLocalReachDensities(point))
      .map((key, point) -> updateLocalOutlierFactors(point))
      .map((key, point) -> clearDisposableSetsAndReturnCurrentScore(point))
      ;

    //lofScores.map((point, lof) -> getTopNOutliers(point, lof)); // let's ignore this function for now...

    // lofScores is a stream of (point, lof) keyvalues where the keys may be repeated
    // convert to ktable to get the latest value per point, hopefully
    // i can query this name
    //KTable<Point, Double> latestLofScores = lofScores.toTable(Materialized.as("latest-lof-scores"));

    // not sure what happens when i turn the table back into a stream
    //latestLofScores.toStream().print(Printed.toFile(config.get("SINK_FILE")));

    // OR...
    // final Serde<String> stringSerde = Serdes.String();
    // lofScores.toStream().to("mouse-outliers-topic", Produced.with(stringSerde, stringSerde));
  }

  public static void processSeq(KStream<String, Point> data, Dotenv config) {
    // This function contains the standalone ILOF algorithm.
    setup(config);
    data.foreach((key, point) -> {
      calculateSymmetricDistances(point,
                                  Optional.ofNullable(config.get("DISTANCE_MEASURE"))
                                  .orElse("EUCLIDEAN"));
      querykNN(point);
      calculateReachDist(point);
      calculateLocalReachDensity(point, false);
      calculateLocalOutlierFactor(point);
      queryReversekNN(point);
      updateReachDists(point);
      updateLocalReachDensities(point);
      updateLocalOutlierFactors(point);
      clearDisposableSetsAndReturnCurrentScore(point);
      getTopNOutliers(point, LOFs.get(point));
    });

    // TODO: need to write labeled data to sink file
    // so write function that decides from lof score whether outlier (1) or not (0)
    // and make stream of flat mapped (key: point, value: label)
    // print stream to sink file then make materialize as topic

  }

  public static void getkNN(Point point) {
    try {
      ArrayList<Pair<Point, Double>> distances = new ArrayList<>();
      pointStore.values().forEach(otherPoint -> {
        if (otherPoint.equals(point)) return;
        double distance = point.getDistanceTo(otherPoint, "EUCLIDEAN");
        distances.add(new Pair<Point, Double>(otherPoint, distance));
      });
      distances.sort(PointComparator.comparator());
      double kdist = 0;
      if (distances.size() > 0) {
        kdist = distances.get(Math.min(k-1, distances.size()-1)).getValue1();
      }
      kDistances.put(point, kdist == 0 ? Double.POSITIVE_INFINITY : kdist);
      int i = k;
      for (; i < distances.size() && distances.get(i).getValue1() == kdist; i++) { }
      PriorityQueue<Pair<Point, Double>> pq = new PriorityQueue<>(PointComparator.comparator().reversed());
      if (distances.size() > 0) {
        distances.subList(0, Math.min(i, distances.size()-1)).forEach(neighbor -> {
          pq.add(neighbor);
        });
      }
      kNNs.put(point, pq);
    } catch (Exception e) {
      System.out.println("getkNN " + e);
    }
  }

  public static void getRds(Point point) {
    try {
      kNNs.get(point).forEach(neighbor -> {
        double reachDist = Math.max(kDistances.get(neighbor.getValue0()), 
                                    point.getDistanceTo(neighbor.getValue0(), "EUCLIDEAN"));
        Pair<Point, Point> pair = new Pair<>(point, neighbor.getValue0());
        reachDistances.put(pair, reachDist);
      });
    } catch (Exception e) {
      System.out.println("getRds " + e);
    }
  }

  public static boolean isNeighborOf(Point query, Point center) {
    try {
      for (Pair<Point,Double> pair : kNNs.get(center)) {
        if (pair.getValue0().equals(query)) {
          return true;
        }
      }
    } catch (Exception e) {
      System.out.println("isNeighborOf " + e);
    }
    return false;
  }

  public static HashSet<Point> getRkNN(Point point) {
    HashSet<Point> rknn = new HashSet<>();
    // does this assume kNNs and kdistances are valid
    // or do i need to recompute from scratch?
    try {
      pointStore.values().forEach(otherPoint -> {
        if (isNeighborOf(point, otherPoint)) {
          rknn.add(otherPoint);
        }
      });
    } catch (Exception e) {
      System.out.println("getRkNN " + e);
    }
    return rknn;
  }

  public static void getLrd(Point point) {
    try {
      double rdSum = 0;
      Iterator<Pair<Point, Double>> neighbors = kNNs.get(point).iterator();
      //System.out.println("lrd 1");
      while (neighbors.hasNext()) {
        //System.out.println("lrd 2");
        Point neighbor = neighbors.next().getValue0();
        //System.out.println("lrd 3");
        Pair<Point, Point> pair = new Pair<>(point, neighbor);
        rdSum += reachDistances.get(pair);
        //System.out.println("lrd 4");
      }
      LRDs.put(point, rdSum == 0 ? Double.POSITIVE_INFINITY : kNNs.get(point).size() / rdSum);
      //System.out.println("lrd 5");
    } catch (Exception e) {
      System.out.println("getLrd  " + e);
    }
  }

  public static void getLof(Point point) {
    try {
      double lrdSum = 0;
      Iterator<Pair<Point, Double>> neighbors = kNNs.get(point).iterator();
      //System.out.println("lof 1");
      while (neighbors.hasNext()) {
        //System.out.println("lof 2");
        lrdSum += LRDs.get(neighbors.next().getValue0());
        //System.out.println("lof 3");
      }
      LOFs.put(point, lrdSum / (LRDs.get(point) * kNNs.get(point).size()));
      //System.out.println("lof 4");
    } catch (Exception e) {
      System.out.println("getLof " + e);
    }
  }

  public static void processScratch(KStream<String, Point> data, Dotenv config) {
    // This function contains the standalone ILOF algorithm.
    setup(config);
    data.foreach((key, point) -> {
      pointStore.put(point, point);
      totalPoints++;
      getkNN(point);
      getRds(point);
      HashSet<Point> update_kdist = getRkNN(point);
      for (Point to_update : update_kdist) {
        //getkDist(to_update);
        // i'm assuming this should be equivalent to just getting the knn again
        getkNN(to_update);
        // but i could write updatekDist() that performs the update logic from querykNN()
      }
      HashSet<Point> update_lrd = new HashSet<>(update_kdist);
      for (Point to_update : update_kdist) {
        for (Pair<Point, Double> neigh : kNNs.get(to_update)) {
          if (neigh.getValue0().equals(point)) {
            continue;
          }
          reachDistances.put(new Pair<>(neigh.getValue0(), to_update), kDistances.get(to_update));
          if (isNeighborOf(to_update, neigh.getValue0())) {
            update_lrd.add(neigh.getValue0()); // i can't seem to align this part with my ilof notes
          }
        }
      }
      HashSet<Point> update_lof = new HashSet<>(update_lrd);
      for (Point to_update : update_lrd) {
        getLrd(to_update);
        update_lof.addAll(getRkNN(to_update));
      }
      for (Point to_update : update_lof) {
        getLof(to_update);
      }
      getLrd(point);
      getLof(point);
      if (totalPoints == 500) {
        LOFs.entrySet().forEach(entry -> {
          System.out.println(entry.getKey() + "" + entry.getValue());
        });
      }
    });

  }
  
    
}
