package capstone;

import java.util.Comparator;
import org.javatuples.Pair;

public class PointComparator {

    public static Comparator<Pair<Point, Double>> comparator() {
      return (o1, o2) -> {
        double v1 = ((Pair<Point, Double>)o1).getValue1();
        double v2 = ((Pair<Point, Double>)o2).getValue1();
        if (v1 < v2) {
          return 1;
        } else if (v1 > v2) {
          return -1;
        } else {
          return 0;
        }
      };
    }
    
}
