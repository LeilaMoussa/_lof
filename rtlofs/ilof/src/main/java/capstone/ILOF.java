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
    System.out.println("start of " + point + " with total points " + totalPoints);
    try {
      System.out.println(point);
      pointStore.put(point, point);
      totalPoints += 1;
      pointStore.values().forEach((otherPoint) -> {
        final double distance = point.getDistanceTo(otherPoint, distanceMeasure);
        symDistances.put(new HashSet<Point>(Arrays.asList(point, otherPoint)), distance);
      });
    } catch (Exception e) {
      System.out.println("1" + e);
    }
    return new KeyValue<Point, Point>(point, point);
  }

  public static KeyValue<Point, Point> querykNN(Point point) {
    try {
      ArrayList<Pair<Point, Double>> distances = new ArrayList<>();
      pointStore.values().forEach(otherPoint -> {
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
          }
        }
      });
      distances.sort(PointComparator.comparator());
      double kdist = distances.get(Math.min(k-1, distances.size()-1)).getValue1(); // maybe k, not k-1, because self is there
      kDistances.put(point, kdist == 0 ? Double.POSITIVE_INFINITY : kdist);
      int i = k;
      for (; i < totalPoints && distances.get(i).getValue1() == kDistances.get(point); i++) { }
      PriorityQueue<Pair<Point, Double>> pq = new PriorityQueue<>(PointComparator.comparator().reversed());
      distances.subList(0, Math.min(i, distances.size()-1)).forEach(neighbor -> {
        if (neighbor.getValue0().equals(point)) return;
        pq.add(neighbor);
      });
      kNNs.put(point, pq);
      neighborhoodCardinalities.put(point, pq.size());
    } catch (Exception e) {
      System.out.println("2" + e);
    }
    return new KeyValue<Point, Point>(point, point);
  }

  public static KeyValue<Point, Point> calculateReachDist(Point point) {
    try {
      kNNs.get(point).forEach(neighbor -> {
        double reachDist = Math.max(kDistances.get(neighbor.getValue0()), symDistances.get(new HashSet<Point>(Arrays.asList(point, neighbor.getValue0()))));
        Pair<Point, Point> pair = new Pair<>(point, neighbor.getValue0());
        Double oldRdIfAny = null;
        if (reachDistances.containsKey(pair)) {
          oldRdIfAny = reachDistances.get(pair);
        }
        reachDistances.put(pair, reachDist);
        if (oldRdIfAny != null && oldRdIfAny != reachDist) {
          reachDistChanged.add(point);
        }
      });
    } catch (Exception e) {
      System.out.println("3" + e);
    }
    return new KeyValue<Point, Point>(point, point);
  }

  public static KeyValue<Point, Point> calculateLocalReachDensity(Point point) {
    try {
      double rdSum = 0;
      Iterator<Pair<Point, Double>> neighbors = kNNs.get(point).iterator();
      while (neighbors.hasNext()) {
        Pair<Point, Point> pair = new Pair<Point, Point>(point, neighbors.next().getValue0());
        rdSum += reachDistances.get(pair); // null exception here because kNN(point) is non empty while rd(point) hasn't been created
        // this is bad because it means iterations may be executed in parallel
        // if this is really the case, i might need to change this entire code and write the whole stream into a hashset at once
        // then just iterate over that set, to make sure each point's processing runs end to end before the next point
      }
      LRDs.put(point, rdSum == 0 ? Double.POSITIVE_INFINITY : neighborhoodCardinalities.get(point) / rdSum);
    } catch (Exception e) {
      System.out.println("4 " + e + " point " + point);
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
      System.out.println("5" + e);
    }
    return new KeyValue<Point, Point>(point, point);
  }

  public static KeyValue<Point, Point> queryReversekNN(Point point) {
    try {
      HashSet<Point> rknns = new HashSet<>();
      pointStore.values().forEach(otherPoint -> {
        double dist = symDistances.get(new HashSet<Point>(Arrays.asList(point, otherPoint)));
        if (kNNs.get(otherPoint).contains(new Pair<Point, Double>(point, dist))) {
          rknns.add(otherPoint);
        }
      });
      RkNNs.put(point, rknns);
    } catch (Exception e) {
      System.out.println("6" + e);
    }
    return new KeyValue<Point, Point>(point, point);
  }

  public static KeyValue<Point, Point> updateReachDists(Point point) {
    try {
      kDistChanged.forEach(somePoint -> {
        kNNs.get(somePoint).forEach(neighbor -> {
          calculateReachDist(neighbor.getValue0());
        });
      });
    } catch (Exception e) {
      System.out.println("7" + e);
    }
    return new KeyValue<Point, Point>(point, point);
  }

  public static KeyValue<Point, Point> updateLocalReachDensities(Point point) {
    try {
      HashSet<Point> target = new HashSet<>();
      target.addAll(neighCardinalityChanged);
      reachDistChanged.forEach(rdChanged -> {
        target.addAll(RkNNs.get(rdChanged));
      });
      target.forEach(toUpdate -> {
        double oldLrd = LRDs.get(toUpdate);
        calculateLocalReachDensity(toUpdate);
        if (oldLrd != LRDs.get(toUpdate)) {
          lrdChanged.add(toUpdate);
        }
      });
    } catch (Exception e) {
      System.out.println("8" + e);
    }
    return new KeyValue<Point, Point>(point, point);
  }

  public static KeyValue<Point, Point> updateLocalOutlierFactors(Point point) {
    try {
      HashSet<Point> target = new HashSet<>();
      target.addAll(lrdChanged);
      lrdChanged.forEach(changed -> {
        target.addAll(RkNNs.get(changed));
      });
      target.forEach(toUpdate -> {
        calculateLocalOutlierFactor(toUpdate);
      });
    } catch (Exception e) {
      System.out.println("9" + e);
    }
    return new KeyValue<Point, Point>(point, point);
  }

  public static KeyValue<Point, Double> clearDisposableSetsAndReturnCurrentScore(Point point) {
    try {
      kDistChanged.clear();
      reachDistChanged.clear();
      neighCardinalityChanged.clear();
      lrdChanged.clear();
      System.out.println("end of " + point + " with total points " + totalPoints);
    } catch (Exception e) {
      System.out.println("10" + e);
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
      System.out.println("11" + e);
    }
    return new KeyValue<Point, Double>(point, lof);
  }

  public static void store(Point point) {
    // TODO make pointStore set instead of map
    pointStore.put(point, point);
  }

  public static void main(String[] args) {
    // TODO: better handling of defaults.
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

    processWithStore(data, dotenv);

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
    setup(config);
    KStream<Point, Double> lofScores = 
      data
      .map((key, formattedPoint) -> calculateSymmetricDistances(formattedPoint, 
                                      Optional.ofNullable(config.get("DISTANCE_MEASURE"))
                                      .orElse("EUCLIDEAN")))
      .map((key, point) -> querykNN(point))
      .map((key, point) -> calculateReachDist(point))
      .map((key, point) -> calculateLocalReachDensity(point))
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

  public static void processWithStore(KStream<String, Point> data, Dotenv config) {
    // This function contains the standalone ILOF algorithm.
    setup(config);
    data.foreach((key, point) -> store(point));

    // TODO: might need to change these function signatures.
    pointStore.values().forEach(point -> {
      calculateSymmetricDistances(point, 
                                      Optional.ofNullable(config.get("DISTANCE_MEASURE"))
                                      .orElse("EUCLIDEAN"));
      querykNN(point);
      calculateReachDist(point);
      calculateLocalReachDensity(point);
      calculateLocalOutlierFactor(point);
      queryReversekNN(point);
      updateReachDists(point);
      updateLocalReachDensities(point);
      updateLocalOutlierFactors(point);
      clearDisposableSetsAndReturnCurrentScore(point);
    });

    // TODO: need to write labeled data to sink file
    // so write function that decides from lof score whether outlier (1) or not (0)
    // and make stream of flat mapped (key: point, value: label)
    // print stream to sink file then make materialize as topic

  }

  public static KStream<Point, Double> ilofRoutine(KStream<String, Point> data, Dotenv config) {
    // This is the routine that other algorithms will use
    setup(config);
    KStream<Point, Double> lofScores = 
      data
      .map((key, formattedPoint) -> calculateSymmetricDistances(formattedPoint, config.get("DISTANCE_MEASURE")))
      .map((key, point) -> querykNN(point))
      .map((key, point) -> calculateReachDist(point))
      .map((key, point) -> calculateLocalReachDensity(point))
      .map((key, point) -> calculateLocalOutlierFactor(point))

      .map((key, point) -> queryReversekNN(point))
      .map((key, point) -> updateReachDists(point))
      .map((key, point) -> updateLocalReachDensities(point))
      .map((key, point) -> updateLocalOutlierFactors(point))
      .map((key, point) -> clearDisposableSetsAndReturnCurrentScore(point));

      return lofScores;
  }
    
}
