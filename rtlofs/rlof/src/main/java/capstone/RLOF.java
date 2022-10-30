package capstone;

import java.util.HashSet;
import org.javatuples.Triplet;

public class RLOF {

    public static HashSet<Triplet<Point, Double, Integer>> blackHoles = new HashSet<>();

    // subscribe to input topic
    // if vps exist, decide whether to isnert new point
    // if insert, pass to ilof
    // ilof outputs a topic of outliers
    // add to window
    // if window size is max, summarize
    // age-based deletion
    
}
