package capstone;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.PriorityQueue;
import com.google.common.collect.MinMaxPriorityQueue;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;
import org.javatuples.Pair;
import org.javatuples.Triplet;
import io.github.cdimascio.dotenv.Dotenv;

public class RLOF {

    public static HashSet<Triplet<Point, Double, Integer>> blackHoles = new HashSet<>(); // Center, Radius, Number
    // profiles of vps, where the keys are the blackhole centers
    public static HashMap<Point, Double> vpKdists = new HashMap<>();
    public static HashMap<Point, Double> vpRds = new HashMap<>();
    public static HashMap<Point, Double> vpLrds = new HashMap<>();
    public static long ts = 0L;
    public static HashMap<Point, Long> pointTimestamps = new HashMap<>();
    // again, key is BH center
    public static HashMap<Point, Long> vpTimestamps = new HashMap<>();
    public static long W;
    public static long MAX_AGE;
    public static String distanceMeasure;
    public static int inlierPercentage;

    public static HashSet<Point> window = new HashSet<>();

    public static HashSet<Triplet<Point, Double, Integer>> findBlackholeIfAny(Point point) {
        HashSet<Triplet<Point, Double, Integer>> found = new HashSet<>();
        for (Triplet<Point, Double, Integer> triplet : blackHoles) {
            double distance = point.getDistanceTo(triplet.getValue0(), distanceMeasure);
            if (distance <= triplet.getValue1()) {
                found.add(triplet);
            }
        }
        return found;
    }

    public static void summarize() {
        final int numberTopInliers = (int)(W * inlierPercentage / 100);
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
            double radius = kDists.get(center);
            HashSet<Point> neighbors = kNNs.get(center); // TODO: value is hashset<point>
            toDelete.addAll(neighbors);
            int number = neighbors.size() + 1;
            blackHoles.add(new Triplet<Point,Double,Integer>(center, radius, number));
            double avgKdist, avgRd, avgLrd;
            for (Point neighbor : neighbors) {
                avgKdist += kDists.get(neighbor);
                avgRd += rds.get(neighbor);
                avgLrd += lrds.get(neighbor);
            }
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
            vpTimestamps.put(center, ts); // ts is only incremented when a neww real point joins
        }
        // TODO: remove profiles of these points from everywhere.
        // might have to make a function for this
        window.removeAll(toDelete);
    }

    public static void updateVps(Triplet<Point, Double, Integer> blackHole, Point point) {
        Point center = blackHole.getValue0();
        int number = blackHole.getValue2();
        double newAvgKdist = (vpKdists.get(center) * number + kDists.get(point)) / (number + 1);
        double newAvgRd = (vpRds.get(center) * number + rds.get(point)) / (number + 1);
        double newAvgLrd = (vpLrds.get(center) * number + lrds.get(point)) / (number + 1);
        vpKdists.put(center, newAvgKdist);
        vpRds.put(center, newAvgRd);
        vpLrds.put(center, newAvgLrd);
        double new_kdist = point.getDistanceTo(blackHole.getValue0(), distanceMeasure);
        blackHoles.remove(blackHole);
        blackHoles.add(new Triplet<Point,Double,Integer>(center, new_kdist, number+1));
    }

    public static void ageBasedDeletion() {
        // but do we do this in batch? Because otherwise, this is equivalent to dequeueing every time we enqueue
        // and in this case since i'm not quite using a queue, it's a loop every time!
        HashSet<Point> toDelete = new HashSet<>();
        pointTimestamps.entrySet().forEach(entry -> {
            if (entry.getValue() > MAX_AGE) {
                toDelete.add(entry.getKey());
            }
        });
        window.removeAll(toDelete);
        // TODO delete profile information of toDelete here
        // now for the vps:
        toDelete.clear();
        vpTimestamps.entrySet().forEach(entry -> {
            if (entry.getValue() > MAX_AGE) {
                toDelete.add(entry.getKey());
            }
        });
        blackHoles.removeIf(bh -> toDelete.contains(bh.getValue0()));
        for (Point x : toDelete) {
            vpKdists.remove(x);
            vpRds.remove(x);
            vpLrds.remove(x);
            vpTimestamps.remove(x);
        }
    }

    public static void setup(Dotenv config) {
        // TODO unify naming convention
        W = Integer.parseInt(config.get("WINDOW"));
        MAX_AGE = Integer.parseInt(config.get("MAX_AGE"));
        distanceMeasure = config.get("DISTANCE_MEASURE");
        inlierPercentage = Integer.parseInt(config.get("INLIER_PERCENTAGE"));
    }
    
    public static void process(KStream<String, Point> data, Dotenv config) {
        // when calling ilof, remember that ilof needs to know about the virtual points too
        // => when calling ilof on a point, insert all the vps "temporarily" into the pointstore that is passed to ilof
        // with coordinatess such that they are positioned to achieve abs(d-R) or sqrt(d²+R²) or d+R
        // or better yet, pass as separate collection indicating their symDistances are fixed and known
        // but they need to be treated like Points though they don't have coordinates
        setup(config);

        data
        .map((key, point) -> {
            HashSet<Triplet<Point, Double, Integer>> triplets = findBlackholeIfAny(point);
            return new KeyValue<Point, HashSet<Triplet<Point, Double, Integer>>>(point, triplets);
        })
        .mapValues((point, triplets) -> {
            ILOF.computeProfileAndMaintainWindow(point);
            // need to retrieve point profile
            // at this point, i need to start fixing global state
            if (triplets.size() == 0) {
                // add to window
                // only increment ts if point was actually added
                pointTimestamps.put(point, ts++);
            } else {
                for (Triplet<Point,Double,Integer> triplet : triplets) {
                    updateVps(triplet, point);   
                }
                // don't insert here into window
            }
            return window.size();
        })
        // which one to do first? summarization or age based deletion when the max window size is exceeded?
        // i feel like age based deletion is slightly less "disruptive"
        .mapValues(windowSize -> {
            if (windowSize >= W) {
                summarize();
            }
            return 0; // decide what to return
        })
        .mapValues(ts -> {
            ageBasedDeletion();
            return window.size();
        })
        ;
    }
}