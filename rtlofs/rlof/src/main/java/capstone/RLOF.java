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

    public static HashSet<Point> window = new HashSet<>();

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
            // TODO make sure size in non zero, which should always be the case as this is an inlier
            avgKdist /= neighbors.size();
            avgRd /= neighbors.size();
            avgLrd /= neighbors.size();

            vpKdists.put(center, avgKdist);
            vpRds.put(center, avgRd);
            vpLrds.put(center, avgLrd);
        }
        // TODO: remove profiles of these points from everywhere.
        window.removeAll(toDelete); */
    }

    public static void updateVps(Triplet<Point, Double, Integer> blackHole, Point point) {
        /* Point center = blackHole.getValue0();
        int number = blackHole.getValue2();
        double newAvgKdist = (vpKdists.get(center) * number + kDists.get(point)) / (number + 1);
        double newAvgRd = (vpRds.get(center) * number + rds.get(point)) / (number + 1);
        double newAvgLrd = (vpLrds.get(center) * number + lrds.get(point)) / (number + 1);
        vpKdists.put(center, newAvgKdist);
        vpRds.put(center, newAvgRd);
        vpLrds.put(center, newAvgLrd); */
    }

    public static void ageBasedDeletion() {
        // could use timestamps, perhaps with a hashmap like this
        HashMap<Point, Long> timestamps = new HashMap<>();
        timestamps.entrySet().forEach(entry-> {
            // what?
        });
    }

    public static void setup(Dotenv config) {

    }
    
    public static void process(KStream<String, Point> data, Dotenv config) {
        // when calling ilof, remember that ilof needs to know about the virtual points too
        // => when calling ilof on a point, insert all the vps "temporarily" into the pointstore that is passed to ilof
        // with coordinatess such that they are positioned to achieve abs(d-R) or sqrt(d²+R²) or d+R
        // or better yet, pass as separate collection indicating their symDistances are fixed and known
        // but they need to be treated like Points though they don't have coordinates

        // it would be pretty great to start using state stores right now, to avoid dealing with this much global state

        // for age-based deletion, see streams tumbling window!

        // define window here
        // pass it to ilof as pointstore
        // need to ensure that profiles in ilof act like singletons: maybe init here and pass in and out of ilof?
        // vp data stays here of course

        data
        .map((key, point) -> {
            Triplet<Point, Double, Integer> triplet = findBlackholeIfAny(point);
            return new KeyValue<Point, Triplet<Point, Double, Integer>>(point, triplet);
        })
        .mapValues((point, triplet) -> {
            ILOF.computeProfileAndMaintainWindow(point);
            // need to retrieve point profile
            // at this point, i need to start fixing global state
            if (triplet == null) {
                // add to window
            } else {
                updateVps(triplet, point);
                // don't insert here into window
            }
            // TODO Change all integers (int, Integer), where applicable and reasonable, to Long
            return window.size();
        })
        // which one to do first? summarization or age based deletion when the max window size is exceeded?
        // i feel like age based deletion is slightly less "disruptive"
        .mapValues(windowSize -> {
            if (windowSize >= Integer.parseInt(config.get("WINDOW"))) {
                summarize();
            }
            return ++ts;
        })
        .mapValues(ts -> {
            // if i'm using timestamps, assign a ts to each point here
            ageBasedDeletion();
            return window.size();
        })
        ;
    }
    
}