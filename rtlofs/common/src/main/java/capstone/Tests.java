package capstone;

import java.util.HashMap;
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
            if (reachDistances.get(new Pair<>(point, neigh)) != point.getDistanceTo(neigh.getValue0(), distanceMeasure) &&
            reachDistances.get(new Pair<>(point, neigh)) != kDistances.get(neigh.getValue0())) {
                return false;
            }
        }
        return true;
    }
}
