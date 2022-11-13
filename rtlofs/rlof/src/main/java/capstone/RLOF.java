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
    public static int ts = 0;

    public static HashSet<Point> window = new HashSet<>(); // TODO: decide how to share window with ILOF

    public static Triplet<Point, Double, Integer> findBlackholeIfAny(Point point) {
        Triplet<Point, Double, Integer> found = null;
        for (Triplet<Point, Double, Integer> triplet : blackHoles) {
            double distance = point.getDistanceTo(triplet.getValue0(), "EUCLIDEAN"); // TODO: distance measure
            if (distance <= triplet.getValue1()) {
                found = triplet;
            }
        }
        return found;
    }

    public static void summarize() {
        // TODO: do the following in some setup function and have these be global
        /* final int w = Integer.parseInt(dotenv.get("WINDOW"));
        final int perc = Integer.parseInt(dotenv.get("INLIER_PERCENTAGE"));
        final int numberTopInliers = (int)(w * perc / 100);
        MinMaxPriorityQueue<Pair<Point, Double>> sorted = MinMaxPriorityQueue
                                                            .orderedBy(PointComparator.comparator().reversed())
                                                            .maximumSize(numberTopInliers)
                                                            .create();
        for (Point point : window) {
            sorted.add(new Pair<Point, Double>(point, LOFs.get(point))); // TODO: decide on how LOFs etc. are shared
        }
        HashSet<Point> toDelete = new HashSet<>();
        for (Pair<Point,Double> inlier : sorted) {
            Point center = inlier.getValue0();
            toDelete.add(center);
            double radius = kDists.get(center); // TODO same here
            HashSet<Point> neighbors = kNNs.get(center); // TODO: probably not hashset<point>
            toDelete.addAll(neighbors);
            int number = neighbors.size() + 1;
            blackHoles.add(new Triplet<Point,Double,Integer>(center, radius, number));
            double avgKdist, avgRd, avgLrd;
            for (Point neighbor : neighbors) {
                avgKdist += kDists.get(neighbor);
                avgRd += rds.get(neighbor);
                avgLrd += lrds.get(neighbor); // TODO
            }
            // TODO make sure size in non zero, which should always be the case as this is an inlier
            avgKdist /= neighbors.size();
            avgRd /= neighbors.size();
            avgLrd /= neighbors.size();

            vpKdists.put(center, avgKdist);
            vpRds.put(center, avgRd);
            vpLrds.put(center, avgLrd);
        }
        window.removeAll(toDelete); */
    }

    public static void updateVps(Triplet<Point, Double, Integer> blackHole, Point point) {
        /* Point center = blackHole.getValue0();
        int number = blackHole.getValue2();
        double newAvgKdist = (vpKdists.get(center) * number + kDists.get(point)) / (number + 1); // TODO
        double newAvgRd = (vpRds.get(center) * number + rds.get(point)) / (number + 1); // TODO
        double newAvgLrd = (vpLrds.get(center) * number + lrds.get(point)) / (number + 1); // TODO
        vpKdists.put(center, newAvgKdist);
        vpRds.put(center, newAvgRd);
        vpLrds.put(center, newAvgLrd); */
    }

    public static void ageBasedDeletion() {

    }

    public static void setup(Dotenv config) {

    }
    
    public static void process(KStream<String, Point> data, Dotenv config) {
        // when calling ilof, remember that ilof needs to know about the virtual points too
        // it would be pretty great to start using state stores right now, to avoid dealing with this much global state

        // for age-based deletion, see streams tumbling window!

        data
        .mapValues((key, point) -> findBlackholeIfAny(point))
        .mapValues((key, triplet) -> {
            if (triplet == null) {
                // add to window, pass to ilof
            } else {
                // update triplet from point's profile (need to run ilof either way)
                // don't insert here
            }
            // TODO Change all integers (int, Integer), where applicable and reasonable, to Long
            return window.size();
        })
        .mapValues(windowSize -> {
            if (windowSize >= Integer.parseInt(config.get("WINDOW"))) {
                summarize();
            }
            return ++ts;
        })
        .mapValues(ts -> {
            if (ts >= Integer.parseInt(config.get("MAX_AGE"))) {
                ageBasedDeletion();
            }
            ts = 0;
            return window.size();
        })
        ;
    }
    
}