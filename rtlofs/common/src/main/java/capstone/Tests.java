package capstone;

import java.util.HashMap;
import java.util.HashSet;
import java.util.PriorityQueue;

import org.javatuples.Pair;

public class Tests {

    public static boolean atLeastKNeighbors(PriorityQueue<Pair<Point, Double>> knn, int k) {
        return knn.size() >= k;
    }

    public static boolean reachDistForEachNeighborHasValidValue(Point point, PriorityQueue<Pair<Point, Double>> knn, HashMap<Pair<Point, Point>, Double> reachDistances,
                                                                HashMap<Point, Double> kDistances, String distanceMeasure) {
        for (Pair<Point, Double> neigh : knn) {
            if (reachDistances.containsKey(new Pair<>(point, neigh.getValue0())) == false) return false;
            // TODO fix this, account for neigh being VP
            if (reachDistances.get(new Pair<>(point, neigh)) != point.getDistanceTo(neigh.getValue0(), distanceMeasure) &&
            reachDistances.get(new Pair<>(point, neigh)) != kDistances.get(neigh.getValue0())) {
                return false;
            }
        }
        return true;
    }

    // TODO does this damn thing even execute?
    public static boolean noVirtualPointsAreToBeUpdated(HashSet<Point> to_update) {
        for (Point p : to_update) {
            if (p.getClass().equals(VPoint.class)) return false;
        }
        return true;
    }
}
