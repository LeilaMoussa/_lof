package capstone;

import java.util.HashMap;
import java.util.HashSet;

import org.apache.kafka.streams.kstream.KStream;
import org.javatuples.Triplet;

public class RLOF {

    public static HashSet<Triplet<Point, Double, Integer>> blackHoles = new HashSet<>(); // Center, Radius, Number
    // profiles of vps, where the keys are the blackhole centers
    public static HashMap<Point, Double> vpKdists = new HashMap<>();
    public static HashMap<Point, Double> vpRds = new HashMap<>();
    public static HashMap<Point, Double> vpLrds = new HashMap<>();

    // if vps exist, decide whether to insert new point
    // if insert, pass to ilof
    // ilof outputs a topic of outliers
    // add to window
    // if window size is max, summarize
    // age-based deletion: see streams tumbling window!

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
        // get all points in window along with their lofs
        // sort asc
        // for each point in x%
        // C = point
        // get its kdist, R = kdist
        // get knn
        // N = size of knn + 1
        // get their average kdist, rd, lrd
        // put these averages in maps
        // delete them from window along with point
        // add (C, R, N) to blackHoles
    }

    public static void updateVps() {

    }

    public static void detectOutliers(KStream<String, Point> data, int k, Integer topNOutliers, Double lofThresh, String distanceMeasure) {
        // when calling ilof, remember that ilof needs to know about the virtual points too
        // it would be pretty great to start using state stores right now, to avoid dealing with this much global state
    }
    
}
