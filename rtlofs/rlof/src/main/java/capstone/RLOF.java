/**
 * RLOF: Score-guided Optimized Realtime Outlier Detection for Data Streams
 * Omar Iraqi, Leila Moussa, Hanan El Bakkali
 * School of Science and Engineering, Al Akhawayn University, Ifrane, Morocco
 * Rabat-IT Center, ENSIAS, Mohammed V University, Rabat, Morocco
 * Email: o.iraqi@aui.ma, l.moussa@aui.ma, hanan.elbakkali@um5.ac.ma
 * 
 * Citation subject to change.
 * 
 * Author: Leila Moussa (l.moussa@aui.ma)
 */

package capstone;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Optional;
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
import org.apache.kafka.streams.kstream.Printed;
import org.javatuples.Pair;
import org.javatuples.Triplet;
import io.github.cdimascio.dotenv.Dotenv;

public class RLOF {

    public static HashSet<Point> window; // Needs to stay under size W
    public static HashMap<HashSet<Point>, Double> symDistances; // As in ILOF.java, this keeps track of the distances between each pair of points
    public static HashMap<Point, PriorityQueue<Pair<Point, Double>>> kNNs; // All these collections here are similar to ILOF too
    public static HashMap<Point, Double> kDistances;
    public static HashMap<Pair<Point, Point>, Double> reachDistances;
    public static HashMap<Point, Double> LRDs;
    public static HashMap<Point, Double> LOFs;

    public static HashSet<Triplet<Point, Double, Integer>> blackHoles; // Center, Radius, Number
    // profiles of vps, where the keys are the blackhole centers
    public static HashMap<Point, Double> vpKdists;
    public static HashMap<Point, Double> vpLrds;
    public static HashMap<Point, Long> vpTimestamps;

    public static long ts = 0L; // keep track of the current timestamp, used for age-based deletion, incremented whenever a new point is added
    public static HashMap<Point, Long> pointTimestamps; // every point in the window has a timestamp
                                                        // real points and virtual points have separate collections to keep track of these timestamps

    public static int k;
    public static long W;
    public static long MAX_AGE;
    public static String DISTANCE_MEASURE;
    public static int INLIER_PERCENTAGE;
    public static int TOP_N;
    public static long totalPoints;
    public static MinMaxPriorityQueue<Pair<Point, Double>> topOutliers;
    public static HashSet<KeyValue<String, Integer>> mapped;
    public static String SINK;

    public static long startTime;

    // This function finds all the blackholes within which a new point falls
    // NOTE: Currently, this is just a linear search, but we'd like to make it smarter
    // We're thinking of applying LSH here too
    public static HashSet<Triplet<Point, Double, Integer>> findBlackholeIfAny(Point point) {
        HashSet<Triplet<Point, Double, Integer>> found = new HashSet<>(); // there can be many such BHs
        try {
            for (Triplet<Point, Double, Integer> triplet : blackHoles) {
                Double distance = point.getDistanceTo(triplet.getValue0(), DISTANCE_MEASURE);
                symDistances.put(new HashSet<Point>(Arrays.asList(point, triplet.getValue0())), distance);
                // point is within a blackhole if the distance between the point and the BH's center is lte to the radius of the blackhole
                if (distance <= triplet.getValue1()) {
                    found.add(triplet);
                }
            }
        } catch (Exception e) {
            System.out.println("findBlackholeIfAny " + e + e.getStackTrace()[0].getLineNumber());
        }
        return found;
    }

    // VP-summarization routine
    public static void summarize() {
        try {
            // TODO: make window a min heap so you don't do this every time
            // in that case, just poll top numberTopInliers from window
            // and you wouldn't need to make pq `sorted` below
            final int numberTopInliers = (int)Math.ceil(window.size() * INLIER_PERCENTAGE / 100.0);
            if (numberTopInliers == 0) return;
            MinMaxPriorityQueue<Pair<Point, Double>> sorted = MinMaxPriorityQueue
                                                            .orderedBy(Comparators.pointComparator())
                                                            .maximumSize(numberTopInliers)
                                                            .create();
            for (Point point : window) {
                sorted.add(new Pair<Point, Double>(point, LOFs.get(point)));
            }
            // Recall: java flag -ea runs assertions
            assert(Tests.isMinHeap(sorted));
            HashSet<Point> toDelete = new HashSet<>();
            for (Pair<Point, Double> inlier : sorted) { // for each top inlier
                Point center = inlier.getValue0();
                assert(Tests.centerIsNotVirtual(center));
                toDelete.add(center); // centers are to be deleted
                double radius = kDistances.get(center); // the initial BH radius is that center's kdist (radius of kNN to be deleted)
                // make hashset of points out of kNN of center
                HashSet<Point> neighbors = new HashSet<>();
                for (Pair<Point, Double> n : kNNs.get(center)) {
                    neighbors.add(n.getValue0());
                }
                toDelete.addAll(neighbors); // neighbors are to be deleted
                int number = neighbors.size() + 1; // number of deleted items is size of kNN + 1 (center)
                assert(Tests.isAtLeast(number, k));
                assert(Tests.blackholeDoesNotAlreadyExist(blackHoles, center));
                blackHoles.add(new Triplet<Point, Double, Integer>(center, radius, number)); // make new BH
                // compute average kdist and lrd based on neighbors only
                double avgKdist = 0, avgLrd = 0;
                for (Point neighbor : neighbors) {
                    // note that neighbors may be virtual points (but centers may not be!)
                    Double kdist, lrd;
                    if (neighbor.getClass().equals(VPoint.class)) {
                        kdist = vpKdists.get(((VPoint)neighbor).center);
                        lrd = vpLrds.get(((VPoint)neighbor).center);
                    } else {
                        kdist = kDistances.get(neighbor);
                        lrd = LRDs.get(neighbor);
                    }
                    avgKdist += kdist;
                    avgLrd += lrd;
                }
                assert(Tests.isPositive(neighbors.size()));
                avgKdist /= neighbors.size();
                avgLrd /= neighbors.size();
                assert(Tests.isPositive(avgKdist));
                assert(Tests.isPositive(avgLrd));

                vpKdists.put(center, avgKdist);
                vpLrds.put(center, avgLrd);
                vpTimestamps.put(center, ts); // the timestamp of the newly created BH is the current ts
            }
            // IMPROVE: disable the following statement and anything similar unless assertions are enabled (?)
            int before = window.size();
            fullyDeleteRealPoints(toDelete);
            assert(Tests.isLessThan(window.size(), before));
        } catch (Exception e) {
            System.out.println("summarize " + e + e.getStackTrace()[0].getLineNumber());
        }
    }

    // This function updates the average density information of blackholes if a new point joins them using the profile of that point
    public static void updateVps(Triplet<Point, Double, Integer> blackHole,
                                Point point,
                                PriorityQueue<Pair<Point, Double>> kNN, // NOTE: is this unused?
                                Double kdistance,
                                HashMap<Pair<Point, Point>, Double> reachDistances, // NOTE: this too? If so, remove! Probably remnants
                                Double lrd) {
        try {
            Point center = blackHole.getValue0();
            int number = blackHole.getValue2();
            // simply update average
            double newAvgKdist = (vpKdists.get(center) * number + kdistance) / (number + 1);
            double newAvgLrd = (vpLrds.get(center) * number + lrd) / (number + 1);

            // TODO: revisit these 2 assumptions, as they did not quite work out the first time
            // can this be proven?
            // assert(Tests.isAtMost(newAvgKdist, vpKdists.get(center))); // average kdist would decrease, i.e. BH area would shrink
            // assert(Tests.isAtLeast(newAvgLrd, vpLrds.get(center))); // average lrd would increase as the BH gets denser

            vpKdists.put(center, newAvgKdist);
            vpLrds.put(center, newAvgLrd);
            // new_kdist is new radius == distance(new point, BH center)
            // NOTE: as I write these comments, I'm second-guessing the significance of shrinking the radius
            // The alternative would be to assume that a BH with a larger area would have a bigger influence...
            // We can very easily change that when we get around to fine-tuning the algorithm.

            // Again, look up the distance in symmetric distance collection, and if not found, compute
            Double new_kdist = symDistances.containsKey(new HashSet<Point>(Arrays.asList(point, blackHole.getValue0()))) ?
                                symDistances.get(new HashSet<Point>(Arrays.asList(point, blackHole.getValue0()))) :
                                point.getDistanceTo(blackHole.getValue0(), DISTANCE_MEASURE);
            assert(Tests.isAtMost(new_kdist, blackHole.getValue1()));
            // To modify the BH's profile, here I remove it then create a new one
            blackHoles.remove(blackHole);
            blackHoles.add(new Triplet<Point,Double,Integer>(center, new_kdist, number+1));
            // reset the BH's timestamp because it's active and ineligible for age-based deletion
            vpTimestamps.put(center, ts);
        } catch (Exception e) {
            System.out.println("updateVps " + e + e.getStackTrace()[0].getLineNumber());
        }
    }

    public static void ageBasedDeletion() {
        try {
            HashSet<Point> toDelete = new HashSet<>();
            // first, delete old real points
            pointTimestamps.entrySet().forEach(entry -> {
                long age = ts - entry.getValue(); // age = current - inception
                if (age > MAX_AGE) {
                    // in case they're an old outlier, i.e. in case they should make it eventually in the top outliers, we don't want to delete and forget them
                    topOutliers.add(new Pair<>(entry.getKey(), LOFs.get(entry.getKey())));
                    toDelete.add(entry.getKey());
                }
            });
            // NOTE: same remark on disabling assertion-related statements
            int before = window.size();
            fullyDeleteRealPoints(toDelete);
            toDelete.clear();
            // then delete vps
            vpTimestamps.entrySet().forEach(entry -> {
                long age = ts - entry.getValue();
                if (age > MAX_AGE) {
                    toDelete.add(entry.getKey());
                }
            });
            fullyDeleteVirtualPoints(toDelete);
            assert(Tests.isAtMost(window.size(), before));
        } catch (Exception e) {
            System.out.println("ageBasedDeletion " + e + e.getStackTrace()[0].getLineNumber());
        }
    }

    // This function deletes all trace of given real points from all collections
    // No need to add comments, as it's very straight forward
    // However, I am not happy with this logic, mainly because it's error prone and computationally expensive
    // We'd like to find a way to make summarization cheaper
    // One way to do that is to decrease the amount of collections we're using.
    // The following 2 functions are a bit of a pain.
    public static void fullyDeleteRealPoints(HashSet<Point> toDelete) {
        try {
            window.removeAll(toDelete);
            for (Point x : toDelete) {
                if (!(x.getClass().equals(VPoint.class))) {
                    // add to sink file output: real points that are summarized (in summarize() or in ageBasedDeletion())
                    mapped.add(new KeyValue<String, Integer>(x.key, labelPoint(x)));
                }

                kNNs.remove(x);
                kDistances.remove(x);
                LRDs.remove(x);
                LOFs.remove(x);
                pointTimestamps.remove(x);
                HashSet<Pair<Point, Point>> keys = new HashSet<>();
                for (Entry<Pair<Point, Point>, Double> entry : reachDistances.entrySet()) {
                    if (entry.getKey().getValue0().equals(x) || entry.getKey().getValue1().equals(x)) {
                        keys.add(entry.getKey());
                    }
                }
                reachDistances.keySet().removeAll(keys);
                for (Entry<Point, PriorityQueue<Pair<Point, Double>>> entry : kNNs.entrySet()) {
                    entry.getValue().removeIf(pair -> pair.getValue0().equals(x));
                }
                HashSet<HashSet<Point>> dkeys = new HashSet<>();
                for (Entry<HashSet<Point>, Double> entry : symDistances.entrySet()) {
                    if (entry.getKey().contains(x)) {
                        dkeys.add(entry.getKey());
                    }
                }
                symDistances.keySet().removeAll(dkeys);
                if (ILOF.hashes != null) {
                    ILOF.hashes.remove(x);
                    ILOF.hashTables.forEach(table -> {
                        table.entrySet().forEach(entry -> {
                            entry.getValue().remove(x);
                        });
                    });
                } else if (ILOF.kdindex != null) {
                    ILOF.kdindex.delete(x.attributesToArray());
                }
            }
        } catch (Exception e) {
            System.out.println("fullyDeleteRealPoints " + e + e.getStackTrace()[0].getLineNumber());
        }
    }

    // This one does the same thing, but with the vp collections
    // again, cumbersome, error prone, and expensive
    public static void fullyDeleteVirtualPoints(HashSet<Point> toDelete) {
        // toDelete is the set of blackhole centers
        try {
            for (Point x : toDelete) {
                vpKdists.remove(x);
                vpLrds.remove(x);
                vpTimestamps.remove(x);

                HashSet<Pair<Point,Point>> keys = new HashSet<>();
                for (Entry<Pair<Point, Point>, Double> entry : reachDistances.entrySet()) {
                    if (entry.getKey().getValue1().equals(x)) {
                        keys.add(entry.getKey());
                    }
                }
                reachDistances.keySet().removeAll(keys);
                for (Entry<Point, PriorityQueue<Pair<Point, Double>>> entry : kNNs.entrySet()) {
                    entry.getValue().removeIf(pair -> pair.getValue0().equals(x));
                }
                HashSet<HashSet<Point>> dkeys = new HashSet<>();
                for (Entry<HashSet<Point>, Double> entry : symDistances.entrySet()) {
                    entry.getKey().forEach(elt -> {
                        if (elt.getClass().equals(VPoint.class) && ((VPoint)elt).center.equals(x)) {
                            dkeys.add(entry.getKey());
                        }
                    });
                }
                symDistances.keySet().removeAll(dkeys);
                // in hashes, hashTables, and kdindex, I have VPoints, not centers, as keys, which makes this inergonomic
                // to delete from hashes, hashTables, and kdindex, i need to look for VPoints whose center equals x
                // hopefully we can find a nicer way to do all this
                HashSet<Point> hkeys = new HashSet<>();
                if (ILOF.hashes != null) {
                    // remove key from hashes if key.class is VPoint and key.center == x
                    for (Entry<Point, ArrayList<Long>> entry : ILOF.hashes.entrySet()) {
                        Point point = entry.getKey();
                        if (point.getClass().equals(VPoint.class) &&
                        ((VPoint)point).center.equals(x)) {
                            hkeys.add(point);
                        }
                    }
                    ILOF.hashes.keySet().removeAll(hkeys);
                    ILOF.hashTables.forEach(table -> {
                        table.entrySet().forEach(entry -> {
                            entry.getValue().removeIf(point -> point.getClass().equals(VPoint.class) &&
                             ((VPoint)point).center.equals(x));
                        });
                    });
                } else if (ILOF.kdindex != null) {
                    // delete from kdindex objects that are instances of VPoint and center is x
                    // gonna need to add functionality to KDTree or KDNode where i can iterate on all nodes
                    // or here, i could derive all possible virtual points from x (the center)
                    // and try to delete each possible vp from the tree
                    Triplet<Point, Double, Integer> correspondingBlackhole = null;
                    for (Triplet<Point, Double, Integer> bh : blackHoles) {
                        if (bh.getValue0().equals(x)) {
                            correspondingBlackhole = bh;
                            break;
                        }
                    }
                    // Here, i'm deriving all possible VPs from the current set of BH's, and deleting all these VPs from kdindex
                    // I'm sure we can find a better design
                    ArrayList<VPoint> vps = ILOF.deriveVirtualPointsFromBlackhole(correspondingBlackhole);
                    vps.forEach(vp -> ILOF.kdindex.delete(vp.attributesToArray()));
                }
                blackHoles.removeIf(bh -> toDelete.contains(bh.getValue0()));
            }
        } catch (Exception e) {
            System.out.println("fullyDeleteVirtualPoints " + e + e.getStackTrace()[0].getLineNumber());
        }
    }

    public static void setup(Dotenv config) {
        window = new HashSet<>();
        symDistances = new HashMap<>();
        kNNs = new HashMap<>();
        kDistances = new HashMap<>();
        reachDistances = new HashMap<>();
        LRDs = new HashMap<>();
        LOFs = new HashMap<>();
        blackHoles = new HashSet<>();
        vpKdists = new HashMap<>();
        vpLrds = new HashMap<>();
        pointTimestamps = new HashMap<>();
        vpTimestamps = new HashMap<>();

        k = Integer.parseInt(config.get("k"));
        W = Integer.parseInt(config.get("WINDOW"));
        MAX_AGE = Integer.parseInt(config.get("MAX_AGE"));
        DISTANCE_MEASURE = config.get("DISTANCE_MEASURE");
        INLIER_PERCENTAGE = Integer.parseInt(config.get("INLIER_PERCENTAGE"));
        TOP_N = Optional.ofNullable(Integer.parseInt(config.get("TOP_N_OUTLIERS"))).orElse(10);
        topOutliers = MinMaxPriorityQueue.orderedBy(Comparators.pointComparator().reversed()).maximumSize(TOP_N).create();
        mapped = new HashSet<>();
        SINK = Utils.buildSinkFilename(config, true);
    }

    // TODO: make shared util (used here and in ILOF, and likely in any other alg)
    public static int labelPoint(Point point) {
        return topOutliers.contains(new Pair<Point, Double>(point, LOFs.get(point))) ? 1 : 0;
      }
    
    public static void process(KStream<String, Point> data, Dotenv config) {
        setup(config);

        data
        .map((key, point) -> {
            // clear labels to be written to sink file to avoid repeating
            mapped.clear();
            // set unique identifier === kafka key
            point.setKey(key);
            totalPoints++;
            if (totalPoints == 1) {
                startTime = System.nanoTime();
            }
            // first, find blackholes this new point belongs to
            HashSet<Triplet<Point, Double, Integer>> triplets = findBlackholeIfAny(point);
            return new KeyValue<Point, HashSet<Triplet<Point, Double, Integer>>>(point, triplets);
        })
        .flatMap((point, triplets) -> {
            // second, get the point's profile, which you'll need either way (whether the point is immediately summarized or not)
            // tentatively add the point to the window, as you might delete it right after if immediately summarized
            // you need to add it though because ILOF uses `window` to populate and operate on `pointStore`
            window.add(point);
            // run ILOF, which literally copies the state of RLOF and operates on it
            // I do not really like this design, as it results in a bad-looking function signature and error-proneness due to aliasing
            // Instead, maybe we can have the collections static in ILOF and used during this RLOF subroutine instantiated only once
            // and emptied in the deletion routine using ILOF.* (basically, what I'm doing with hashes, hashTables, and kdindex)
            ILOF.ilofSubroutineForRlof(point,
                                    window,
                                    symDistances,
                                    kNNs,
                                    kDistances,
                                    reachDistances,
                                    LRDs,
                                    LOFs,
                                    blackHoles,
                                    vpKdists,
                                    vpLrds,
                                    config);

            if (triplets.size() == 0) { // if the point is NOT immediately summarized
                pointTimestamps.put(point, ts++);
            } else {
                // otherwise, use the point to update those BH's
                for (Triplet<Point,Double,Integer> triplet : triplets) {
                    // NOTE: I don't think kNNs.get(point) and reachDistances are actually needed in the following function
                    updateVps(triplet, point, kNNs.get(point), kDistances.get(point), reachDistances, LRDs.get(point));
                }
                // and summarize point by deleting all trace of it
                fullyDeleteRealPoints(new HashSet<Point>(Arrays.asList(point)));
                assert(Tests.pointHasNotAffectedRlof(point, window, kNNs, kDistances, reachDistances, LRDs, LOFs, symDistances));
                // NOTE: I'm realizing at this stage, point should have been fully deleted, so LOFs.get(point) should be null
                // while the next function call, labelPoint, uses LOF.get(point)
                // Is this a mistake I overlook? If so, swap statements.

                // add to sink file output: immediately summarized pts
                mapped.add(new KeyValue<String, Integer>(point.key, labelPoint(point)));
            }
            // third, check window size
            if (window.size() >= W) {
                // start by summarizing if needed
                summarize();
            }
            if (window.size() >= W) {
                // then delete old points if that wasn't enough
                ageBasedDeletion();
            }

            // IMPROVE: might want to check if limit of W is always met here
            // assert(window.size() < W);

            if (totalPoints == Integer.parseInt(config.get("TOTAL_POINTS"))) { // if at end of stream
                long estimatedEndTime = System.nanoTime();

                for (Point x : window) {
                  topOutliers.add(new Pair<Point, Double>(x, LOFs.get(x)));
                };
                // IMPROVE: verbose mode here too
                assert(Tests.isMaxHeap(topOutliers));
                for (Point x : window) {
                  System.out.println(x + " " + LOFs.get(x));
                  // System.out.println(x.key + " " + labelPoint(x));
                  
                  // add to sink: points that were never deleted
                  mapped.add(new KeyValue<String, Integer>(x.key, labelPoint(x)));
                }
                System.out.println("Estimated time elapsed ms " + (estimatedEndTime - startTime) / 1000000);
            }
            return mapped;
        })
        .print(Printed.toFile(SINK));
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

        KStream<String, Point> data = rawData.flatMapValues(value -> Arrays.asList(Utils.parse(value,
                                                                                " ",
                                                                                Integer.parseInt(dotenv.get("DIMENSIONS")))));

        process(data, dotenv);

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();
    }
}
