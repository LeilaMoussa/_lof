import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.ArrayDeque;
import java.util.ArrayList;


public class ILOF {
  // unify naming convention here

  // actually, forget about mem ceil and summ for now
  // public static ArrayDeque<Point> window = new ArrayDeque<>(); // maybe look at EvictingQueue or CircularFifoQueue instead
  // consider making all these in-memory collections implement KeyValueStore
  public static HashMap<Point, Point> pointStore = new HashMap<>();
  //public static final String SYMMETRIC_DISTANCE_STORE_NAME = "SymmetricDistanceStore";
  public static HashMap<SymPair<Point>, Double> symDistances = new HashMap<>();
  //public static final String KNN_STORE_NAME = "kNNStore";
  public static HashMap<Point, HashSet<Point>> kNNs = new HashMap<>();
  public static HashMap<Point, Double> kDistances = new HashMap<>();
  public static HashMap<AsymPair<Point, Point>, Double> reachDistances = new HashMap<>();
  public static HashMap<Point, Integer> neighborhoodCardinalities = new HashMap<>();
  public static HashMap<Point, Double> LRDs = new HashMap<>();
  public static HashMap<Point, Double> LOFs = new HashMap<>();
  public static HashMap<Point, HashSet<Point>> RkNNs = new HashMap<>();
  public static final int K = 3; // configable
  // get rid of totalPoints
  public static int totalPoints = 0;

  public static class Point {
    double x;
    double y;

    Point(double x, double y) {
      this.x = x;
      this.y = y;
    }

    public double getDistanceTo(Point other) {
      // distance measure should also be configable
      return Math.pow(this.x - other.x, 2) + Math.pow(this.y - other.y, 2);
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
  }

  // honestly, i could just replace these pairs with a set and a tuple
  public static class SymPair<E> {
    final Point a;
    final Point b;

    public SymPair(Point a, Point b) {
      this.a = a.x <= b.x ? a : b;
      this.b = a.x <= b.x ? b : a;
    }

    @Override
    public boolean equals(Object other) {
      return other != null && other instanceof SymPair 
      && ((SymPair<Point>)other).a.equals(this.a) && ((SymPair<Point>)other).b.equals(this.b);
    }

    @Override
    public int hashCode() {
      return a.hashCode() + b.hashCode(); // do i need something better?
    }
  }

  public static class AsymPair<E, T> {
    E a;
    T b;

    public AsymPair(E a, T b) {
      this.a = a;
      this.b = b;
    }

    // must define equals and hashcode
    // yeah screw that, i'll use an array
  }

  public static class DistanceComparator<T> implements Comparator<T> {

    @Override
    public int compare(Object o1, Object o2) {
      // risky!
      double d1 = ((AsymPair<Point, Double>)o1).b;
      double d2 = ((AsymPair<Point, Double>)o2).b;
      if (d1 < d2) {
        return -1;
      } else if (d1 > d2) {
        return 1;
      }
      return 0;
    }
  }

  public static Point format(String line) {
    String[] split = line.toLowerCase().split("\\W+");
    Point formatted = new Point(Double.parseDouble(split[0]), Double.parseDouble(split[1]));
    pointStore.put(formatted, formatted);
    totalPoints++;
    return formatted;
  }

  public static KeyValue<Point, Point> calculateSymmetricDistances(Point point) {
    pointStore.values().forEach((otherPoint) -> {
      final double distance = point.getDistanceTo(otherPoint);
      symDistances.put(new SymPair<Point>(point, otherPoint), distance); // i want this to be an append only state store
      // each point will give me a collection of distances, so i need to aggregate all these distances into a single store
    });
    return new KeyValue<Point, Point>(point, point);
  }

  public static KeyValue<Point, Point> querykNN(Point point) {
    ArrayList<AsymPair<Point, Double>> distances = new ArrayList<>();
    pointStore.values().forEach(otherPoint -> {
      Double distance = symDistances.get(new SymPair<>(point, otherPoint));
      if (distance > 0) distances.add(new AsymPair<Point, Double>(otherPoint, distance));
    });
    distances.sort(new DistanceComparator<>());
    kDistances.put(point, distances.get(K-1).b); // risky too
    int i;
    for (i = K; i < totalPoints && distances.get(i).b == kDistances.get(point); i++) { }
    ArrayList<Point> neighbors = new ArrayList<Point>();
    distances.subList(0, i).forEach(asymPair -> neighbors.add(asymPair.a)); // risky too
    kNNs.put(point, new HashSet<Point>(neighbors));
    neighborhoodCardinalities.put(point, i);
    return new KeyValue<Point, Point>(point, point);
  }

  public static KeyValue<Point, Point> calculateReachDist(Point point) {
    kNNs.get(point).forEach(neighbor -> {
      // must double check reach dist calculations and usages throughout (didn't swap operands)
      double reachDist = Math.max(kDistances.get(neighbor), symDistances.get(new SymPair<>(point, neighbor)));
      AsymPair<Point, Point> pair = new AsymPair<>(point, neighbor);
      reachDistances.put(pair, reachDist);
    });
    return new KeyValue<Point, Point>(point, point);
  }

  public static KeyValue<Point, Point> calculateLocalReachDensity(Point point) {
    double rdSum = 0;
    Iterator<Point> neighbors = kNNs.get(point).iterator();
    while (neighbors.hasNext()) {
      rdSum += reachDistances.get(new AsymPair<Point, Point>(point, neighbors.next()));
    }
    LRDs.put(point, rdSum == 0 ? Double.POSITIVE_INFINITY : neighborhoodCardinalities.get(point) / rdSum);
    return new KeyValue<Point, Point>(point, point);
  }

  public static KeyValue<Point, Point> calculateLocalOutlierFactor(Point point) {
    double lrdSum = 0;
    Iterator<Point> neighbors = kNNs.get(point).iterator();
    while (neighbors.hasNext()) {
      lrdSum += LRDs.get(neighbors.next());
    }
    LOFs.put(point, lrdSum / (LRDs.get(point) * neighborhoodCardinalities.get(point)));
    return new KeyValue<Point, Point>(point, point);
  }

  public static KeyValue<Point, Point> queryReversekNN(Point point) {
    RkNNs.put(point, new HashSet<Point>());
    pointStore.values().forEach(otherPoint -> {
      if (kNNs.get(otherPoint).contains(point)) { // wait, is kNNs updated for all other points?
        RkNNs.get(point).add(otherPoint);
      }
    });
    // or make a stream aggregating RkNNs.get(point)?
    return new KeyValue<Point, Point>(point, point);
  }

  public static void main(String[] args) {
      Properties props = new Properties();
      props.put(StreamsConfig.APPLICATION_ID_CONFIG, "ilof-application");
      props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
      props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
      props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

      StreamsBuilder builder = new StreamsBuilder();
      KStream<String, String> textLines = builder.stream("mouse-topic"); // next time: why isn't mouse-topic getting populated?
      // KTable<String, Long> wordCounts = textLines
      //     .flatMapValues(textLine -> Arrays.asList(textLine.toLowerCase().split("\\W+")))
      //     .groupBy((key, word) -> word)
      //     .count(Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("counts-store"));
      // wordCounts.toStream().to("mouse-count-topic", Produced.with(Serdes.String(), Serdes.Long()));

      // INSERT PHASE
      // format
      textLines.flatMapValues(textLine -> Arrays.asList(format(textLine)))
      // sym dists
      .map((key, formattedPoint) -> calculateSymmetricDistances(formattedPoint))
      // knn
      .map((key, point) -> querykNN(point))
      // rd
      .map((key, point) -> calculateReachDist(point))
      // lrd
      .map((key, point) -> calculateLocalReachDensity(point))
      // lof
      .map((key, point) -> calculateLocalOutlierFactor(point))
      // UPDATE / MAINTAIN PHASE
      // get RkNN
      .map((key, point) -> queryReversekNN(point))
      // use eq. 5 to update k dists and knns, try to make querykNN reusable
      ;

      KafkaStreams streams = new KafkaStreams(builder.build(), props);
      streams.start();
  }
    
}
