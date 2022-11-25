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
        System.out.println("1");
        return knn.size() >= k;
    }

    public static boolean reachDistForEachNeighborHasValidValue(Point point, PriorityQueue<Pair<Point, Double>> knn,
                                                                HashMap<Pair<Point, Point>, Double> reachDistances,
                                                                HashMap<Point, Double> kDistances,
                                                                HashMap<Point, Double> vpKdists, String distanceMeasure) {
        System.out.println("2");
        for (Pair<Point, Double> neigh : knn) {
            if (reachDistances.containsKey(new Pair<>(point, neigh.getValue0())) == false) {
                System.out.println("2.1");
                return false;
            }
            if (point.getClass().equals(VPoint.class)) {
                System.out.println("2.2");
                return false;
            }
            Double kdist;
            if (neigh.getValue0().getClass().equals(VPoint.class)) {
                kdist = vpKdists.get(((VPoint)neigh.getValue0()).center);
            } else {
                kdist = kDistances.get(neigh.getValue0());
            }
            if (reachDistances.get(new Pair<>(point, neigh.getValue0())).equals(point.getDistanceTo(neigh.getValue0(), distanceMeasure)) == false &&
            reachDistances.get(new Pair<>(point, neigh.getValue0())).equals(kdist) == false) {
                System.out.println("2.3");
                return false;
            }
            if (reachDistances.get(new Pair<>(point, neigh.getValue0())) < 0) {
                System.out.println("2.4");
                return false;
            }
        }
        return true;
    }

    public static boolean noVirtualPointsAreToBeUpdated(HashSet<Point> to_update) {
        System.out.println("3");
        for (Point p : to_update) {
            if (p.getClass().equals(VPoint.class)) return false;
        }
        return true;
    }

    public static boolean isSortedAscending(ArrayList<Pair<Point, Double>> distances) {
        System.out.println("4");
        for (int i = 0; i < distances.size()-1; i++) {
            if (distances.get(i).getValue1() > distances.get(i+1).getValue1()) return false;
        }
        return true;
    }

    public static boolean isMaxHeap(PriorityQueue<Pair<Point, Double>> pq) {
        System.out.println("5");
        PriorityQueue<Pair<Point, Double>> copy = new PriorityQueue<>(PointComparator.comparator().reversed());
        copy.addAll(pq);
        if (pq.size() == 0) return true;
        double assumed_max = copy.poll().getValue1();
        while (copy.peek() != null) {
            if (copy.poll().getValue1() > assumed_max) return false;
        }
        return true;
    }

    public static boolean isMaxHeap(MinMaxPriorityQueue<Pair<Point, Double>> pq) {
        System.out.println("6");
        MinMaxPriorityQueue<Pair<Point, Double>> copy = MinMaxPriorityQueue
                                                        .orderedBy(PointComparator.comparator().reversed())
                                                        .maximumSize(pq.size())
                                                        .create();
        copy.addAll(pq);
        if (pq.size() == 0) return true;
        double assumed_max = copy.poll().getValue1();
        while (copy.peek() != null) {
            if (copy.poll().getValue1() > assumed_max) return false;
        }
        return true;
    }

    public static boolean isMinHeap(MinMaxPriorityQueue<Pair<Point, Double>> pq) {
        System.out.println("7");
        MinMaxPriorityQueue<Pair<Point, Double>> copy = MinMaxPriorityQueue
                                                        .orderedBy(PointComparator.comparator())
                                                        .maximumSize(pq.size())
                                                        .create();
        copy.addAll(pq);
        if (pq.size() == 0) return true;
        double assumed_min = copy.poll().getValue1();
        while (copy.peek() != null) {
            if (copy.poll().getValue1() < assumed_min) return false;
        }
        return true;
    }

    public static boolean blackholeDoesNotAlreadyExist(HashSet<Triplet<Point,Double,Integer>> bhs, Point center) {
        System.out.println("8");
        for (Triplet<Point,Double,Integer> bh : bhs) {
            if (bh.getValue0().equals(center)) return false;
        }
        return true;
    }

    public static boolean centerIsNotVirtual(Point center) {
        System.out.println("9");
        return !(center.getClass().equals(VPoint.class));
    }

    // stupid? sure
    // when these pass and if i have time, i'll make them less dumb
    public static boolean isAtLeast(int n, int k) {
        System.out.println("10");
        return n >= k;
    }

    public static boolean isAtLeast(double n, double k) {
        System.out.println("11 " + n + " " + k);
        return n >= k;
    }

    public static boolean isPositive(int n) {
        System.out.println("12");
        return n > 0;
    }

    public static boolean isPositive(double n) {
        System.out.println("13");
        return n > 0;
    }

    public static boolean isLessThan(int a, int b) {
        System.out.println("14 " + a + " " + b);
        return a < b;
    }

    public static boolean isAtMost(double a, double b) {
        System.out.println("15");
        return a <= b;
    }

    public static boolean pointNotInWindow(HashSet<Point> window, Point point) {
        System.out.println("16");
        return !window.contains(point);
    }

    public static boolean isEq(int a, int b) {
        System.out.println("17 with " + a + " " + b);
        return a == b;
    }

}
