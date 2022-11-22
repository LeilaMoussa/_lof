package capstone;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map.Entry;
import java.util.PriorityQueue;
import java.util.Properties;

import com.google.common.collect.MinMaxPriorityQueue;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.javatuples.Pair;
import org.javatuples.Triplet;
import io.github.cdimascio.dotenv.Dotenv;

public class RLOF {

    public static HashSet<Point> window;
    public static HashMap<Point, PriorityQueue<Pair<Point, Double>>> kNNs;
    public static HashMap<Point, Double> kDistances;
    public static HashMap<Pair<Point, Point>, Double> reachDistances;
    public static HashMap<Point, Double> LRDs;
    public static HashMap<Point, Double> LOFs;

    public static HashSet<Triplet<Point, Double, Integer>> blackHoles; // Center, Radius, Number
    // does this defeat the whole purpose of summarization?
    public static HashSet<VPoint> vps;
    // profiles of vps, where the keys are the blackhole centers
    public static HashMap<Point, Double> vpKdists;
    public static HashMap<Point, Double> vpRds;
    public static HashMap<Point, Double> vpLrds;

    public static long ts = 0L;
    public static HashMap<Point, Long> pointTimestamps;
    // again, key is BH center
    public static HashMap<Point, Long> vpTimestamps;

    public static long W;
    public static long MAX_AGE;
    public static String DISTANCE_MEASURE;
    public static int INLIER_PERCENTAGE;

    public static HashSet<Triplet<Point, Double, Integer>> findBlackholeIfAny(Point point) {
        HashSet<Triplet<Point, Double, Integer>> found = new HashSet<>();
        try {
            for (Triplet<Point, Double, Integer> triplet : blackHoles) {
                double distance = point.getDistanceTo(triplet.getValue0(), DISTANCE_MEASURE);
                if (distance <= triplet.getValue1()) {
                    found.add(triplet);
                }
            }
        } catch (Exception e) {
            System.out.println("findBlackholeIfAny " + e + e.getStackTrace()[0].getLineNumber());
        }
        return found;
    }

    public static double getAverageRdtoNeighbors(Point point,
                                                PriorityQueue<Pair<Point, Double>> kNN,
                                                HashMap<Pair<Point, Point>, Double> rds) {
        double ans = 0;
        try {
            for (Pair<Point,Double> pair : kNN) {
                Point neigh = pair.getValue0();
                ans += rds.get(new Pair<>(point, neigh));
            }
            return ans / kNN.size();
        } catch (Exception e) {
            System.out.println("getAverageRdtoNeighbors " + e + e.getStackTrace()[0].getLineNumber());
        }
        return 0;
    }

    public static void summarize() {
        try {
            final int numberTopInliers = (int)(W * INLIER_PERCENTAGE / 100);
            MinMaxPriorityQueue<Pair<Point, Double>> sorted = MinMaxPriorityQueue
                                                                .orderedBy(PointComparator.comparator().reversed())
                                                                .maximumSize(numberTopInliers)
                                                                .create();
            for (Point point : window) {
                sorted.add(new Pair<Point, Double>(point, LOFs.get(point)));
            }
            HashSet<Point> toDelete = new HashSet<>();
            for (Pair<Point,Double> inlier : sorted) {
                Point center = inlier.getValue0();
                toDelete.add(center);
                double radius = kDistances.get(center);
                HashSet<Point> neighbors = new HashSet<>();
                for (Pair<Point,Double> n : kNNs.get(center)) {
                    neighbors.add(n.getValue0());
                }
                toDelete.addAll(neighbors);
                int number = neighbors.size() + 1;
                blackHoles.add(new Triplet<Point,Double,Integer>(center, radius, number));
                // maybe make V*vps here
                // so much memory consumption!
                double avgKdist = 0, avgRd = 0, avgLrd = 0;
                for (Point neighbor : neighbors) {
                    avgKdist += kDistances.get(neighbor);
                    // here it's not necessary to pass these global references anyway
                    // but it's made necessary by updateVps (global state is not affected by ILOF if we're updating vps)
                    avgRd += getAverageRdtoNeighbors(neighbor, kNNs.get(neighbor), reachDistances);
                    avgLrd += LRDs.get(neighbor);
                }
                // hack:
                if (neighbors.size() == 0) {
                    System.out.println("neighbors size should not be 0!");
                    System.exit(1);
                }
                avgKdist /= neighbors.size();
                avgRd /= neighbors.size();
                avgLrd /= neighbors.size();

                vpKdists.put(center, avgKdist);
                vpRds.put(center, avgRd);
                vpLrds.put(center, avgLrd);
                vpTimestamps.put(center, ts);
            }
            fullyDeleteRealPoints(toDelete);
        } catch (Exception e) {
            System.out.println("summarize " + e + e.getStackTrace()[0].getLineNumber());
        }
    }

    public static void updateVps(Triplet<Point, Double, Integer> blackHole,
                                Point point,
                                PriorityQueue<Pair<Point, Double>> kNN,
                                Double kdistance,
                                HashMap<Pair<Point, Point>, Double> reachDistances,
                                Double lrd) {
        try {
            Point center = blackHole.getValue0();
            int number = blackHole.getValue2();
            double newAvgKdist = (vpKdists.get(center) * number + kdistance) / (number + 1);
            double newAvgRd = (vpRds.get(center) * number + getAverageRdtoNeighbors(point, kNN, reachDistances)) / (number + 1);
            double newAvgLrd = (vpLrds.get(center) * number + lrd) / (number + 1);
            vpKdists.put(center, newAvgKdist);
            vpRds.put(center, newAvgRd);
            vpLrds.put(center, newAvgLrd);
            double new_kdist = point.getDistanceTo(blackHole.getValue0(), DISTANCE_MEASURE);
            blackHoles.remove(blackHole);
            blackHoles.add(new Triplet<Point,Double,Integer>(center, new_kdist, number+1));
            vpTimestamps.put(center, 0L);
        } catch (Exception e) {
            System.out.println("updateVps " + e + e.getStackTrace()[0].getLineNumber());
        }
    }

    public static void ageBasedDeletion() {
        // but do we do this in batch? Because otherwise, this is equivalent to dequeueing every time we enqueue
        // and in this case since i'm not quite using a queue, it's a loop every time!
        try {
            HashSet<Point> toDelete = new HashSet<>();
            pointTimestamps.entrySet().forEach(entry -> {
                if (entry.getValue() > MAX_AGE) {
                    toDelete.add(entry.getKey());
                }
            });
            fullyDeleteRealPoints(toDelete);
            // now for the vps:
            toDelete.clear();
            vpTimestamps.entrySet().forEach(entry -> {
                if (entry.getValue() > MAX_AGE) {
                    toDelete.add(entry.getKey());
                }
            });
            fullyDeleteVirtualPoints(toDelete);
        } catch (Exception e) {
            System.out.println("ageBasedDeletion " + e + e.getStackTrace()[0].getLineNumber());
        }
    }

    public static void fullyDeleteRealPoints(HashSet<Point> toDelete) {
        try {
            window.removeAll(toDelete);
            for (Point x : toDelete) {
                kNNs.remove(x);
                kDistances.remove(x);
                LRDs.remove(x);
                LOFs.remove(x);
                pointTimestamps.remove(x);
                HashSet<Pair<Point,Point>> keys = new HashSet<>();
                for (Entry<Pair<Point,Point>,Double> entry : reachDistances.entrySet()) {
                    if (entry.getKey().getValue0().equals(x) || entry.getKey().getValue1().equals(x)) {
                        keys.add(entry.getKey());
                    }
                }
                for (Entry<Point,PriorityQueue<Pair<Point,Double>>> entry : kNNs.entrySet()) {
                    entry.getValue().removeIf(pair -> pair.getValue0().equals(x));
                }
                reachDistances.keySet().removeAll(keys);
            }
        } catch (Exception e) {
            System.out.println("fullyDeleteRealPoints " + e + e.getStackTrace()[0].getLineNumber());
        }
    }

    public static void fullyDeleteVirtualPoints(HashSet<Point> toDelete) {
        // toDelete is the set of blackhole centers
        try {
            blackHoles.removeIf(bh -> toDelete.contains(bh.getValue0()));
            for (Point x : toDelete) {
                vpKdists.remove(x);
                vpRds.remove(x);
                vpLrds.remove(x);
                vpTimestamps.remove(x);
            }
        } catch (Exception e) {
            System.out.println("fullyDeleteVirtualPoints " + e + e.getStackTrace()[0].getLineNumber());
        }
    }

    public static void setup(Dotenv config) {
        window = new HashSet<>();
        kNNs = new HashMap<>();
        kDistances = new HashMap<>();
        reachDistances = new HashMap<>();
        LRDs = new HashMap<>();
        LOFs = new HashMap<>();
        blackHoles = new HashSet<>();
        vpKdists = new HashMap<>();
        vpRds = new HashMap<>();
        vpLrds = new HashMap<>();
        pointTimestamps = new HashMap<>();
        vpTimestamps = new HashMap<>();

        W = Integer.parseInt(config.get("WINDOW"));
        MAX_AGE = Integer.parseInt(config.get("MAX_AGE"));
        DISTANCE_MEASURE = config.get("DISTANCE_MEASURE");
        INLIER_PERCENTAGE = Integer.parseInt(config.get("INLIER_PERCENTAGE"));
    }
    
    public static void process(KStream<String, Point> data, Dotenv config) {
        setup(config);

        data
        .map((key, point) -> {
            HashSet<Triplet<Point, Double, Integer>> triplets = findBlackholeIfAny(point);
            return new KeyValue<Point, HashSet<Triplet<Point, Double, Integer>>>(point, triplets);
        })
        .foreach((point, triplets) -> {
            if (triplets.size() == 0) {
                // shallow copy, i.e. mutate original collections
                window.add(point);
                ILOF.ilofSubroutineForRlof(point,
                                    window,
                                    kNNs,
                                    kDistances,
                                    reachDistances,
                                    LRDs,
                                    LOFs,
                                    blackHoles,
                                    config);
                pointTimestamps.put(point, ts++);
            } else {
                // deep copy, i.e. don't mutate collections
                HashSet<Point> deepWindow = new HashSet<>(window);
                deepWindow.add(point);
                HashMap<Point, PriorityQueue<Pair<Point, Double>>> deepkNNs = new HashMap<>(kNNs);
                HashMap<Point, Double> deepkDistances = new HashMap<>(kDistances);
                HashMap<Pair<Point, Point>, Double> deepReachDistances = new HashMap<>(reachDistances);
                HashMap<Point, Double> deepLrds = new HashMap<>(LRDs);
                HashMap<Point, Double> deepLofs = new HashMap<>(LOFs);
                HashSet<Triplet<Point, Double, Integer>> deepBlackHoles = new HashSet<>(blackHoles);
                ILOF.ilofSubroutineForRlof(point,
                                    deepWindow,
                                    deepkNNs,
                                    deepkDistances,
                                    deepReachDistances,
                                    deepLrds,
                                    deepLofs,
                                    deepBlackHoles,
                                    config);
                for (Triplet<Point,Double,Integer> triplet : triplets) {
                    updateVps(triplet, point, deepkNNs.get(point), deepkDistances.get(point),
                                deepReachDistances, deepLrds.get(point));
                }
            }
            if (window.size() >= W) {
                summarize();
            }
            if (window.size() >= W) {
                ageBasedDeletion();
            }
        });
    }

    public static void main(String[] args) {
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
}