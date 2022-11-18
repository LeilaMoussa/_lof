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
import java.util.ArrayList;
import java.util.Optional;

import org.javatuples.Pair;

import io.github.cdimascio.dotenv.Dotenv;

public class ILOF {

  public static HashSet<Point> pointStore;
  public static HashMap<Point, PriorityQueue<Pair<Point, Double>>> kNNs;
  public static HashMap<Point, Double> kDistances;
  public static HashMap<Pair<Point, Point>, Double> reachDistances;
  public static HashMap<Point, Double> LRDs;
  public static HashMap<Point, Double> LOFs;

  public static int k;
  public static int topN;
  public static int topPercent;
  public static String distanceMeasure;
  public static String nnsTechnique;
  
  public static MinMaxPriorityQueue<Pair<Point, Double>> topOutliers;
  public static long totalPoints;

  public static void getTarsosLshkNN(Point point) {
    // TODO fix folder structure and packages to be able to use LSH
    HashFamily hashFamily;
    switch (distanceMeasure) {
      case "EUCLIDEAN": hashFamily = new EuclidianHashFamily(/*radiusEuclidean,dimensions*/); break;
      case "MANHATTAN": hashFamily = new CityBlockHashFamily(/*radiusCityBlock,dimensions*/); break;
      default: System.out.println("Unsupported distance measure.");
    }
    // params:
    // List<Vector> dataset
    // HashFamily family
    // int numberOfHashes
    // int numberOfHashTables
    // List<Vector> queries
    // int numberOfNeighbours = k (but again, not quite)

    lshSearch(dataset, family, numberOfHashes, numberOfHashTables, Arrays.asList(), numberOfNeighbours);
  }

  public static void getFlatkNN(Point point) {
    try {
      ArrayList<Pair<Point, Double>> distances = new ArrayList<>();
      pointStore.forEach(otherPoint -> {
        if (otherPoint.equals(point)) return;
        double distance = point.getDistanceTo(otherPoint, distanceMeasure);
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
        // TODO: could turn into addAll for conciseness.
        distances.subList(0, Math.min(i, distances.size())).forEach(neighbor -> {
          pq.add(neighbor);
        });
      }
      kNNs.put(point, pq);
    } catch (Exception e) {
      System.out.println("getFlatkNN " + e);
    }
  }

  public static void getkNN(Point point, String nnsTechnique) {
    switch (nnsTechnique) {
      case "FLAT": getFlatkNN(point); return;
      case "LSH": getTarsosLshkNN(point); return;
      default: System.out.println("Unsupported nearest neighbor search technique.");
    }
  }

  public static void getRds(Point point) {
    try {
      kNNs.get(point).forEach(neighbor -> {
        double reachDist = Math.max(kDistances.get(neighbor.getValue0()), 
                                    point.getDistanceTo(neighbor.getValue0(), distanceMeasure));
        Pair<Point, Point> pair = new Pair<>(point, neighbor.getValue0());
        reachDistances.put(pair, reachDist);
      });
    } catch (Exception e) {
      System.out.println("getRds " + e);
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
      System.out.println("getRkNN " + e);
    }
    return rknn;
  }

  public static HashSet<Point> computeRkNN(Point point) {
    HashSet<Point> rknn = new HashSet<>();
    try {
      pointStore.forEach(x -> {
        if (x.equals(point)) return;
        double dist = point.getDistanceTo(x, distanceMeasure);
        if (dist <= kDistances.get(x)) {
          rknn.add(x);
        }
      });
    } catch (Exception e) {
      System.out.println("computeRkNN " + e);
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
        lrdSum += LRDs.get(neighbors.next().getValue0());
      }
      LOFs.put(point, lrdSum / (LRDs.get(point) * kNNs.get(point).size()));
    } catch (Exception e) {
      System.out.println("getLof " + e + e.getStackTrace()[0].getLineNumber());
    }
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
    k = Optional.ofNullable(Integer.parseInt(config.get("k"))).orElse(3);
    topN = Optional.ofNullable(Integer.parseInt(config.get("TOP_N_OUTLIERS"))).orElse(10);
    topPercent = Optional.ofNullable(Integer.parseInt(config.get("TOP_PERCENT_OUTLIERS"))).orElse(5);
    distanceMeasure = config.get("DISTANCE_MEASURE");
    topOutliers = MinMaxPriorityQueue.orderedBy(PointComparator.comparator().reversed()).maximumSize(topN).create();
    totalPoints = 0;
    nnsTechnique = config.get("ANNS");
  }

  public static void computeProfileAndMaintainWindow(Point point) {
    // This function assumes active points are in pointStore
    getkNN(point, nnsTechnique);
    getRds(point);
    HashSet<Point> update_kdist = computeRkNN(point);
    for (Point to_update : update_kdist) {
      // TODO: i could write updatekDist() that performs the update logic from querykNN()
      // for slightly better performance => i should do this (i.e. push and pop logic)
      getkNN(to_update, nnsTechnique);
    }
    HashSet<Point> update_lrd = new HashSet<>(update_kdist);
    for (Point to_update : update_kdist) {
      for (Pair<Point, Double> neigh : kNNs.get(to_update)) {
        reachDistances.put(new Pair<>(neigh.getValue0(), to_update), kDistances.get(to_update));
        // NOTE: following not from ILOF paper, but without it, reach_dist(old, new) wouldn't exist.
        reachDistances.put(new Pair<>(to_update, neigh.getValue0()),
                          Math.max(
                            to_update.getDistanceTo(neigh.getValue0(), distanceMeasure),
                            kDistances.get(neigh.getValue0())
                          ));
        
        if (neigh.getValue0().equals(point)) {
          continue;
        }
        // NOTE: in ILOF paper, thiss statement is conditional (if to_update is neighbor of neigh).
        update_lrd.add(neigh.getValue0());
        // NOTE: following is not from paper either but from notes.
        for (Pair<Point,Double> y : kNNs.get(neigh.getValue0())) {
          update_lrd.add(y.getValue0());
        }
      }
    }
    HashSet<Point> update_lof = new HashSet<>(update_lrd);
    for (Point to_update : update_lrd) {
      getLrd(to_update);
      update_lof.addAll(getRkNN(to_update));
    }
    // NOTE: iN ILOF paper, this was right before getLof(), but getLof(to_update) needs lrd(new).
    getLrd(point);
    for (Point to_update : update_lof) {
      if (to_update.equals(point)) continue;
      getLof(to_update);
    }
    getLof(point);
  }

  public static void process(KStream<String, Point> data, Dotenv config) {
    setup(config);
    data.foreach((key, point) -> {
      pointStore.add(point);
      totalPoints++;
      computeProfileAndMaintainWindow(point);
      if (totalPoints == 500) {
        pointStore.forEach(x -> {
          System.out.println(x + "" + kNNs.get(x).size() + " " + LRDs.get(x) + " " + LOFs.get(x));
        });
      }
    });

    // TODO: need to write labeled data to sink file
    // so write function that decides from lof score whether outlier (1) or not (0)
    // and make stream of flat mapped (key: point, value: label)
    // print stream to sink file then make materialize as topic

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
}
