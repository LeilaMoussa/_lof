import org.apache.kafka.common.errors.DuplicateBrokerRegistrationException;
import org.apache.kafka.common.errors.NoReassignmentInProgressException;
import org.apache.kafka.common.serialization.Serde;
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
import java.util.PriorityQueue;
import java.util.Properties;
import java.util.Set;
import java.util.ArrayDeque;
import java.util.ArrayList;

import org.javatuples.Pair;

public class ILOF {
  // unify naming convention here

  // maybe look at EvictingQueue or CircularFifoQueue for mem ceil
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

  public static class DistanceComparator<T> implements Comparator<T> {

    @Override
    public int compare(Object o1, Object o2) {
      double d1 = ((Pair<Point, Double>)o1).getValue1();
      double d2 = ((Pair<Point, Double>)o2).getValue1();
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
      symDistances.put(new HashSet<Point>(Arrays.asList(point, otherPoint)), distance);
    });
    return new KeyValue<Point, Point>(point, point);
  }

  public static KeyValue<Point, Point> querykNN(Point point) {
    // VERY IMPORTANT: how is the stream executed? required to know when to clear neighCardinalityChanged
    // because it changes from one instance of the maintenance phase to another
    // maybe add an operation at the end that clears the data structures that are only used for one iteration
    ArrayList<Pair<Point, Double>> distances = new ArrayList<>();
    pointStore.values().forEach(otherPoint -> {
      double distance = symDistances.get(new HashSet<Point>(Arrays.asList(point, otherPoint)));
      if (distance > 0) {
        distances.add(new Pair<Point, Double>(otherPoint, distance));
        // update all other kNNs to be able to get RkNN later
        boolean cardChanged = false;
        if (distance < kDistances.get(otherPoint)) {
          while (kNNs.get(otherPoint).peek().getValue1() == kDistances.get(otherPoint)) {
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
    distances.sort(new DistanceComparator<>());
    kDistances.put(point, distances.get(K-1).getValue1());
    int i;
    for (i = K; i < totalPoints && distances.get(i).getValue1() == kDistances.get(point); i++) { }
    PriorityQueue<Pair<Point, Double>> pq = new PriorityQueue<>(new DistanceComparator<>().reversed());
    distances.subList(0, i).forEach(neighbor -> {
      pq.add(neighbor);
    });
    kNNs.put(point, pq);
    neighborhoodCardinalities.put(point, i);
    return new KeyValue<Point, Point>(point, point);
  }

  public static KeyValue<Point, Point> calculateReachDist(Point point) {
    kNNs.get(point).forEach(neighbor -> {
      // must double check reach dist calculations and usages throughout (didn't swap operands)
      double reachDist = Math.max(kDistances.get(neighbor.getValue0()), symDistances.get(new HashSet<Point>(Arrays.asList(point, neighbor.getValue0()))));
      Pair<Point, Point> pair = new Pair<>(point, neighbor.getValue0());
      // this method is also called in the maintain phase
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

      final Serde<String> stringSerde = Serdes.String();

      // INSERT PHASE
      // format
      KTable<String, PriorityQueue<Pair<Point, Double>>> topOutliers = 
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
      // the best I could think of right now is update k-dist for everyone, as
      // i would have no way of knowing the RkNN of new point without updating everyone's kNN first
      // which means iterating over everyone anyway
      // UPDATE: modify querykNN to update kNNs of all points as i compute their distances to new point
      .map((key, point) -> queryReversekNN(point))
      // use eq. 5 to update k dists and knns, try to make querykNN reusable
      // .map((key, point) -> updatekDists(point))
      // update rd
      .map((key, point) -> updateReachDists(point))
      // update lrd
      .map((key, point) -> updateLocalReachDensities(point))
      // update lof
      .map((key, point) -> updateLocalOutlierFactors(point))
      // clear *changed collections (?)
      // i need to ascertain whether the same collection is used concurrently at different stages of the pipeline
      // top N aggregation
      .groupBy((key, point) -> {
        return new KeyValue<Point, Double>(point, LOFs.get(point));
      })
      .aggregate(
        // the initializer
        () -> new PriorityQueue<>(new DistanceComparator<>().reversed()), // might want to rename this comparator

        // the "add" aggregator
        (point, lofScore, top) -> {
          top.add(new Pair<Point, Double>(point, lofScore));
          return top;
        },

        // the "remove" aggregator
        (point, lofScore, top) -> {
          top.remove(new Pair<Point, Double>(point, lofScore));
          return top;
        },

        Materialized.with(stringSerde, new PriorityQueueSerde<>(new DistanceComparator<>().reversed(), valueAvroSerde)) // just plug in the same confluence and avro deps
        )
      ;


      final int topN = 10; // configable!
      final KTable<String, String> topViewCounts = topOutliers
        .mapValues(queue -> {
          final StringBuilder sb = new StringBuilder();
          for (int i = 0; i < topN; i++) {
            final Pair<Point, Double> record = queue.poll();
            if (record == null) {
              break;
            }
            sb.append(record.getValue0().toString());
            sb.append(" : ");
            sb.append(record.getValue1().toString());
            sb.append("\n");
          }
          return sb.toString();
        });

      topViewCounts.toStream().to("mouse-outliers-topic", Produced.with(stringSerde, stringSerde));


      KafkaStreams streams = new KafkaStreams(builder.build(), props);
      // streams.cleanUp(); // ?
      streams.start();
  }
    
}
