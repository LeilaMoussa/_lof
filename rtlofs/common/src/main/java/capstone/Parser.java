package capstone;

import java.util.ArrayList;

public class Parser {

    public static Point parse(String rawData, String sep, int d) {
        String[] split = rawData.toLowerCase().split(sep);
        // Assuming all attributes are doubles.
        // TODO: account for different data types in .env.
        ArrayList<Double> parsed = new ArrayList<>(d);
        for (String word : split) {
            parsed.add(Double.parseDouble(word));
        }
        return new Point(d, parsed);
    }
    
}
