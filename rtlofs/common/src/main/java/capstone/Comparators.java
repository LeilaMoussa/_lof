package capstone;

import java.util.Comparator;
import org.javatuples.Pair;

public class Comparators {

    public static Comparator<Pair<Point, Double>> pointComparator() {
      return (o1, o2) -> {
        Double v1 = ((Pair<Point, Double>)o1).getValue1();
        Double v2 = ((Pair<Point, Double>)o2).getValue1();
        return Double.compare(v1, v2);
      };
    }

    public static Comparator<Pair<Long, Integer>> hashComparator() {
      return (o1, o2) -> {
        Integer v1 = ((Pair<Long, Integer>)o1).getValue1();
        Integer v2 = ((Pair<Long, Integer>)o2).getValue1();
        return Integer.compare(v1, v2);
      };
    }
    
}
