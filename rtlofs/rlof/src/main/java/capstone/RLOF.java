package capstone;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map.Entry;
import java.util.PriorityQueue;
import com.google.common.collect.MinMaxPriorityQueue;

import org.apache.kafka.streams.KeyValue;
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
        for (Triplet<Point, Double, Integer> triplet : blackHoles) {
            double distance = point.getDistanceTo(triplet.getValue0(), DISTANCE_MEASURE);
            if (distance <= triplet.getValue1()) {
                found.add(triplet);
            }
        }
        return found;
    }

    public static double getAverageRdtoNeighbors(Point point) {
        double ans = 0;
        for (Pair<Point,Double> pair : kNNs.get(point)) {
            Point neigh = pair.getValue0();
            ans += reachDistances.get(new Pair<>(point, neigh));
        }
        return ans / kNNs.get(point).size();
    }

    public static void summarize() {
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
                avgRd += getAverageRdtoNeighbors(neighbor);
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
            vpTimestamps.put(center, ts); // ts is only incremented when a new real point joins
        }
        fullyDeleteRealPoints(toDelete);
    }

    public static void updateVps(Triplet<Point, Double, Integer> blackHole, Point point) {
        Point center = blackHole.getValue0();
        int number = blackHole.getValue2();
        double newAvgKdist = (vpKdists.get(center) * number + kDistances.get(point)) / (number + 1);
        double newAvgRd = (vpRds.get(center) * number + getAverageRdtoNeighbors(point)) / (number + 1);
        double newAvgLrd = (vpLrds.get(center) * number + LRDs.get(point)) / (number + 1);
        vpKdists.put(center, newAvgKdist);
        vpRds.put(center, newAvgRd);
        vpLrds.put(center, newAvgLrd);
        double new_kdist = point.getDistanceTo(blackHole.getValue0(), DISTANCE_MEASURE);
        blackHoles.remove(blackHole);
        blackHoles.add(new Triplet<Point,Double,Integer>(center, new_kdist, number+1));
        vpTimestamps.put(center, 0L);
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
        fullyDeleteRealPoints(toDelete);
        // now for the vps:
        toDelete.clear();
        vpTimestamps.entrySet().forEach(entry -> {
            if (entry.getValue() > MAX_AGE) {
                toDelete.add(entry.getKey());
            }
        });
        fullyDeleteVirtualPoints(toDelete);
    }

    public static void fullyDeleteRealPoints(HashSet<Point> toDelete) {
        window.removeAll(toDelete);
        for (Point x : toDelete) {
            kNNs.remove(x);
            kDistances.remove(x);
            LRDs.remove(x);
            LOFs.remove(x);
            pointTimestamps.remove(x);
            // This is not great code...
            for (Entry<Pair<Point,Point>,Double> entry : reachDistances.entrySet()) {
                if (entry.getKey().getValue0().equals(x) || entry.getKey().getValue1().equals(x)) {
                    reachDistances.remove(entry.getKey()); // probably bad to delete within loop?
                }
            }
        }
    }

    public static void fullyDeleteVirtualPoints(HashSet<Point> toDelete) {
        // toDelete is the set of blackhole centers
        blackHoles.removeIf(bh -> toDelete.contains(bh.getValue0()));
        for (Point x : toDelete) {
            vpKdists.remove(x);
            vpRds.remove(x);
            vpLrds.remove(x);
            vpTimestamps.remove(x);
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
        // when calling ilof, remember that ilof needs to know about the virtual points too
        // => when calling ilof on a point, insert all the vps "temporarily" into the pointstore that is passed to ilof
        // with coordinates such that they are positioned to achieve abs(d-R) or sqrt(d²+R²) or d+R
        // or better yet, pass as separate collection indicating their symDistances are fixed and known
        // but they need to be treated like Points though they don't have coordinates
        setup(config);

        data
        .map((key, point) -> {
            HashSet<Triplet<Point, Double, Integer>> triplets = findBlackholeIfAny(point);
            return new KeyValue<Point, HashSet<Triplet<Point, Double, Integer>>>(point, triplets);
        })
        .mapValues((point, triplets) -> {
            // HACK, but probably won't make it prettier any time soon.
            ILOF.ilofSubroutineForRlof(point,
                                    window,
                                    kNNs,
                                    kDistances,
                                    reachDistances,
                                    LRDs,
                                    LOFs,
                                    blackHoles);
            if (triplets.size() == 0) {
                window.add(point);
                pointTimestamps.put(point, ts++);
            } else {
                for (Triplet<Point,Double,Integer> triplet : triplets) {
                    updateVps(triplet, point);   
                }
            }
            return window.size();
        })
        // which one to do first? summarization or age based deletion when the max window size is exceeded?
        // i *feel* like age based deletion is slightly less "disruptive"
        // the following decisions are a little bit arbitrary
        .mapValues(windowSize -> {
            if (windowSize >= W) {
                summarize();
            }
            return window.size();
        })
        .mapValues(windowSize -> {
            if (windowSize >= W) {
                ageBasedDeletion();
            }
            return window.size();
        })
        ;
    }
}