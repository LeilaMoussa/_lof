package capstone;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.PriorityQueue;

import com.google.common.collect.MinMaxPriorityQueue;

import org.javatuples.Pair;
import org.javatuples.Triplet;

public class Tests {

    public static boolean atLeastKNeighbors(PriorityQueue<Pair<Point, Double>> knn, int k) {
        return knn.size() >= k;
    }

    public static boolean reachDistForEachNeighborHasValidValue(Point point, PriorityQueue<Pair<Point, Double>> knn,
                                                                HashMap<Pair<Point, Point>, Double> reachDistances,
                                                                HashMap<Point, Double> kDistances,
                                                                HashMap<Point, Double> vpKdists, String distanceMeasure) {
        for (Pair<Point, Double> neigh : knn) {
            if (reachDistances.containsKey(new Pair<>(point, neigh.getValue0())) == false) return false;
            if (point.getClass().equals(VPoint.class)) return false;
            double kdist;
            if (neigh.getValue0().getClass().equals(VPoint.class)) {
                kdist = vpKdists.get(((VPoint)neigh.getValue0()).center);
            } else {
                kdist = kDistances.get(neigh.getValue0());
            }
            if (reachDistances.get(new Pair<>(point, neigh)) != point.getDistanceTo(neigh.getValue0(), distanceMeasure) &&
            reachDistances.get(new Pair<>(point, neigh)) != kdist) {
                return false;
            }
            if (reachDistances.get(new Pair<>(point, neigh)) < 0) return false;
        }
        return true;
    }

    public static boolean noVirtualPointsAreToBeUpdated(HashSet<Point> to_update) {
        for (Point p : to_update) {
            if (p.getClass().equals(VPoint.class)) return false;
        }
        return true;
    }

    public static boolean isSortedAscending(ArrayList<Pair<Point, Double>> distances) {
        for (int i = 0; i < distances.size()-1; i++) {
            if (distances.get(i).getValue1() > distances.get(i+1).getValue1()) return false;
        }
        return true;
    }

    public static boolean isMaxHeap(PriorityQueue<Pair<Point, Double>> pq) {
        double assumed_max = pq.poll().getValue1();
        while (pq.peek() != null) {
            if (pq.poll().getValue1() > assumed_max) return false;
        }
        return true;
    }

    public static boolean isMaxHeap(MinMaxPriorityQueue<Pair<Point, Double>> pq) {
        double assumed_max = pq.poll().getValue1();
        while (pq.peek() != null) {
            if (pq.poll().getValue1() > assumed_max) return false;
        }
        return true;
    }

    public static boolean isMinHeap(MinMaxPriorityQueue<Pair<Point, Double>> pq) {
        double assumed_min = pq.poll().getValue1();
        while (pq.peek() != null) {
            if (pq.poll().getValue1() < assumed_min) return false;
        }
        return true;
    }

    public static boolean blackholeDoesNotAlreadyExist(HashSet<Triplet<Point,Double,Integer>> bhs, Point center) {
        for (Triplet<Point,Double,Integer> bh : bhs) {
            if (bh.getValue0().equals(center)) return false;
        }
        return true;
    }

}
