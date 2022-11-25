package capstone;

import java.util.Comparator;
import org.javatuples.Pair;

public class PointComparator {

    public static Comparator<Pair<Point, Double>> comparator() {
      return (o1, o2) -> {
        Double v1 = ((Pair<Point, Double>)o1).getValue1();
        Double v2 = ((Pair<Point, Double>)o2).getValue1();
        return Double.compare(v1, v2);
      };
    }
    
}
