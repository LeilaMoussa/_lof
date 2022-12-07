package capstone;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.PriorityQueue;

import com.google.common.collect.MinMaxPriorityQueue;

import be.tarsos.lsh.Vector;

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

    public static boolean pointHasNotAffectedRlof(Point point, HashSet<Point> window, 
                                                HashMap<Point, PriorityQueue<Pair<Point, Double>>> kNNs, HashMap<Point, Double> kDistances, 
                                                HashMap<Pair<Point, Point>, Double> reachDistances, HashMap<Point, Double> LRDs, HashMap<Point, Double> LOFs, 
                                                HashMap<HashSet<Point>, Double> symDistances) {
        System.out.println("18");
        if (window.contains(point)) {
            System.out.println("18.1");
            return false;
        }
        if (kNNs.containsKey(point)) {
            System.out.println("18.2");
            return false;
        }
        if (kDistances.containsKey(point)) {
            System.out.println("18.3");
            return false;
        }
        if (LRDs.containsKey(point)) {
            System.out.println("18.4");
            return false;
        }
        if (LOFs.containsKey(point)) {
            System.out.println("18.5");
            return false;
        }
        for (Entry<Point,PriorityQueue<Pair<Point,Double>>> entry : kNNs.entrySet()) {
            for (Pair<Point,Double> value : entry.getValue()) {
                if (value.getValue0().equals(point)) {
                    System.out.println("18.6");
                    return false;
                }
            }
        }
        for (Entry<Pair<Point,Point>,Double> entry : reachDistances.entrySet()) {
            if (entry.getKey().getValue0().equals(point)) {
                System.out.println("18.7");
                return false;
            }
            if (entry.getKey().getValue1().equals(point)) {
                System.out.println("18.8");
                return false;
            }
        }
        for (Entry<HashSet<Point>,Double> entry : symDistances.entrySet()) {
            if (entry.getKey().contains(point)) {
                System.out.println("18.9");
                return false;
            }
        }
        return true;
    }

    public static boolean pointNotInDataset(Point point, List<Vector> dataset) {
        // TODO null exception happens somewhere here -- no time to check
        System.out.println("19");
        Vector q = point.toVector();
        for (Vector v : dataset) {
            if (Arrays.equals(v.getValues(), q.getValues())) {
                return false;
            }
        }
        return true;
    }

    public static boolean expectVirtualPointInDataset(List<Vector> dataset, int bhs, int d) {
        System.out.println("20");
        int c = 0;
        for (Vector v : dataset) {
            if (v.virtual) c++;
        }
        return c == bhs * 2 * d;
    }

    public static boolean pointNotInNeighbors(Point point, List<Point> ns) {
        System.out.println("21");
        return !(ns.contains(point));
    }

}
