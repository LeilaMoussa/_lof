package capstone;

import java.util.Comparator;
import org.javatuples.Pair;

public class PointComparator {

    // public static final Comparator<Pair<Point, Double>> instance =
    //   (o1, o2) -> (int) (((Pair<Point, Double>)o1).getValue1() - ((Pair<Point, Double>)o2).getValue1());

    public static Comparator<Pair<Point, Double>> comparator() {
      return (o1, o2) -> (int) (((Pair<Point, Double>)o1).getValue1() - ((Pair<Point, Double>)o2).getValue1());
    }

    public static Comparator<Pair<Point, Double>> reverseComparator() {
      return (o1, o2) -> (int) (((Pair<Point, Double>)o2).getValue1() - ((Pair<Point, Double>)o1).getValue1());
  }
    
}
