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
import com.google.common.collect.MinMaxPriorityQueue;

import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.PriorityQueue;
import java.util.Properties;
import java.util.Set;
import java.util.ArrayList;

import org.javatuples.Pair;

public class ILOF {

  public static HashMap<Point, Point> pointStore = new HashMap<>();
  public static HashMap<Set<Point>, Double> symDistances = new HashMap<>();
  public static HashMap<Point, PriorityQueue<Pair<Point, Double>>> kNNs = new HashMap<>();
  public static HashMap<Point, Double> kDistances = new HashMap<>();
  public static HashMap<Pair<Point, Point>, Double> reachDistances = new HashMap<>();
  public static HashMap<Point, Integer> neighborhoodCardinalities = new HashMap<>();
  public static HashMap<Point, Double> LRDs = new HashMap<>();
  public static HashMap<Point, Double> LOFs = new HashMap<>();
  public static HashMap<Point, HashSet<Point>> RkNNs = new HashMap<>();

  public static HashSet<Point> kDistChanged = new HashSet<>();
  public static HashSet<Point> reachDistChanged = new HashSet<>();
  public static HashSet<Point> neighCardinalityChanged = new HashSet<>();
  public static HashSet<Point> lrdChanged = new HashSet<>();
  public static final int K = 3; // make configable
  public static final int topN = 10; // config
  public static final Comparator<Pair<Point, Double>> comparator =
      (o1, o2) -> (int) (((Pair<Point, Double>)o1).getValue1() - ((Pair<Point, Double>)o2).getValue1());
  public static MinMaxPriorityQueue<Pair<Point, Double>> topOutliers = MinMaxPriorityQueue.orderedBy(comparator.reversed()).maximumSize(topN).create();
  public static int totalPoints = 0;

  public static class Point {
    double x;
    double y;

    Point(double x, double y) {
      this.x = x;
      this.y = y;
    }

    public double getDistanceTo(Point other) {
      // configable
      return Math.sqrt(Math.pow(this.x - other.x, 2) + Math.pow(this.y - other.y, 2));
    }

    @Override
    public boolean equals(Object other) {
      return other != null && other instanceof Point &&
          ((Point) other).x == this.x && ((Point) other).y == this.y;
    }

    @Override
    public int hashCode() {
      return (new Double(this.x).toString() + new Double(this.y).toString()).hashCode();
    }

    @Override
    public String toString() {
      return this.x + " " + this.y;
    }
  }

  public static Point format(String line) {
    // split by some regex; must know input stream encoding
    String[] split = line.toLowerCase().split(" ");
    Point formatted = new Point(Double.parseDouble(split[0]), Double.parseDouble(split[1]));
    //System.out.println(formatted);
    pointStore.put(formatted, formatted);
    totalPoints++;
    return formatted;
  }

  public static KeyValue<Point, Point> calculateSymmetricDistances(Point point) {
    pointStore.values().forEach((otherPoint) -> {
      //if (otherPoint.equals(point)) return;
      final double distance = point.getDistanceTo(otherPoint);
      symDistances.put(new HashSet<Point>(Arrays.asList(point, otherPoint)), distance);
    });
    return new KeyValue<Point, Point>(point, point);
  }

  public static KeyValue<Point, Point> querykNN(Point point) {
    ArrayList<Pair<Point, Double>> distances = new ArrayList<>();
    pointStore.values().forEach(otherPoint -> {
      //if (otherPoint.equals(point)) return;
      double distance = symDistances.get(new HashSet<Point>(Arrays.asList(point, otherPoint)));
      //if (distance > 0)
      distances.add(new Pair<Point, Double>(otherPoint, distance));
      // update all other kNNs to be able to get RkNN later
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
    distances.sort(comparator);
    double kdist = distances.get(Math.min(K-1, distances.size()-1)).getValue1();
    kDistances.put(point, kdist == 0 ? Double.POSITIVE_INFINITY : kdist);
    int i = K;
    for (; i < totalPoints && distances.get(i).getValue1() == kDistances.get(point); i++) { }
    PriorityQueue<Pair<Point, Double>> pq = new PriorityQueue<>(comparator.reversed());
    distances.subList(0, Math.min(i, distances.size()-1)).forEach(neighbor -> {
      pq.add(neighbor);
    });
    kNNs.put(point, pq);
    neighborhoodCardinalities.put(point, i);
    return new KeyValue<Point, Point>(point, point);
  }

  public static KeyValue<Point, Point> calculateReachDist(Point point) {
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
    return new KeyValue<Point, Point>(point, point);
  }

  public static KeyValue<Point, Point> calculateLocalReachDensity(Point point) {
    double rdSum = 0;
    Iterator<Pair<Point, Double>> neighbors = kNNs.get(point).iterator();
    while (neighbors.hasNext()) {
      rdSum += reachDistances.get(new Pair<Point, Point>(point, neighbors.next().getValue0()));
    }
    LRDs.put(point, rdSum == 0 ? Double.POSITIVE_INFINITY : neighborhoodCardinalities.get(point) / rdSum);
    return new KeyValue<Point, Point>(point, point);
  }

  public static KeyValue<Point, Point> calculateLocalOutlierFactor(Point point) {
    double lrdSum = 0;
    Iterator<Pair<Point, Double>> neighbors = kNNs.get(point).iterator();
    while (neighbors.hasNext()) {
      lrdSum += LRDs.get(neighbors.next().getValue0());
    }
    LOFs.put(point, lrdSum / (LRDs.get(point) * neighborhoodCardinalities.get(point)));
    return new KeyValue<Point, Point>(point, point);
  }

  public static KeyValue<Point, Point> queryReversekNN(Point point) {
    HashSet<Point> rknns = new HashSet<>();
    pointStore.values().forEach(otherPoint -> {
      double dist = symDistances.get(new HashSet<Point>(Arrays.asList(point, otherPoint)));
      if (kNNs.get(otherPoint).contains(new Pair<Point, Double>(point, dist))) {
        rknns.add(otherPoint);
      }
    });
    RkNNs.put(point, rknns);
    return new KeyValue<Point, Point>(point, point);
  }

  public static KeyValue<Point, Point> updateReachDists(Point point) {
    kDistChanged.forEach(somePoint -> {
      kNNs.get(somePoint).forEach(neighbor -> {
        calculateReachDist(neighbor.getValue0());
      });
    });
    return new KeyValue<Point, Point>(point, point);
  }

  public static KeyValue<Point, Point> updateLocalReachDensities(Point point) {
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
    return new KeyValue<Point, Point>(point, point);
  }

  public static KeyValue<Point, Point> updateLocalOutlierFactors(Point point) {
    HashSet<Point> target = new HashSet<>();
    target.addAll(lrdChanged);
    lrdChanged.forEach(changed -> {
      target.addAll(RkNNs.get(changed));
    });
    target.forEach(toUpdate -> {
      calculateLocalOutlierFactor(toUpdate);
    });
    if (totalPoints == 500) {
      LOFs.entrySet().forEach(entry -> {
        System.out.println(entry.getKey() + " : " + entry.getValue());
      });
    }
    return new KeyValue<Point, Point>(point, point);
  }

  public static KeyValue<Point, Double> clearDisposableSets(Point point) {
    kDistChanged.clear();
    reachDistChanged.clear();
    neighCardinalityChanged.clear();
    lrdChanged.clear();
    return new KeyValue<Point, Double>(point, LOFs.get(point));
  }

  public static KeyValue<Point, Double> getTopNOutliers(Point point, Double lof) {
    // priority queue of fixed size
    topOutliers.add(new Pair<Point, Double>(point, lof));
    if (totalPoints == 500) {
      System.out.println("TOP");
      while (topOutliers.size() > 0) {
        Pair<Point, Double> max = topOutliers.poll();
        System.out.println(max.getValue0() + " : " + max.getValue1());
      }
    }
    return new KeyValue<Point, Double>(point, lof);
  }

  public static void main(String[] args) {
      Properties props = new Properties();
      props.put(StreamsConfig.APPLICATION_ID_CONFIG, "ilof-application");
      props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
      props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
      props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

      StreamsBuilder builder = new StreamsBuilder();
      KStream<String, String> textLines = builder.stream("mouse-py-topic");

      //final Serde<String> stringSerde = Serdes.String();

      // INSERT PHASE
      KStream<Point, Double> lofScores = 
      textLines.flatMapValues(textLine -> Arrays.asList(format(textLine)))
      .map((key, formattedPoint) -> calculateSymmetricDistances(formattedPoint))
      .map((key, point) -> querykNN(point))
      .map((key, point) -> calculateReachDist(point))
      .map((key, point) -> calculateLocalReachDensity(point))
      .map((key, point) -> calculateLocalOutlierFactor(point))

      // UPDATE / MAINTAIN PHASE
      .map((key, point) -> queryReversekNN(point))
      .map((key, point) -> updateReachDists(point))
      .map((key, point) -> updateLocalReachDensities(point))
      .map((key, point) -> updateLocalOutlierFactors(point))
      // clear sets that are used for one iteration and return pairs of points and lof scores
      .map((key, point) -> clearDisposableSets(point))
      .map((point, lof) -> getTopNOutliers(point, lof))
      ;

      // forget about outputting/serializing for now

      //topOutliers.toStream().to("mouse-outliers-topic", Produced.with(stringSerde, stringSerde));

      KafkaStreams streams = new KafkaStreams(builder.build(), props);
      // streams.cleanUp(); // ?
      streams.start();
  }
    
}
