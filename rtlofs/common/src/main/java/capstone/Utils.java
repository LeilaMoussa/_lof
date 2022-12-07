package capstone;

import java.util.ArrayList;

import io.github.cdimascio.dotenv.Dotenv;

public class Utils {

    public static Point parse(String rawData, String sep, int d) {
        return new Point(d, attributesFromString(rawData, sep, d));
    }

    public static ArrayList<Double> attributesFromString(String rawData, String sep, int d) {
        String[] split = rawData.toLowerCase().split(sep);
        ArrayList<Double> parsed = new ArrayList<>(d);
        for (String word : split) {
            Double doubleVal = null;
            try {
                doubleVal = Double.parseDouble(word);
            } catch (NumberFormatException nfe) {
                doubleVal = word.hashCode() * 1.0;
            }
            parsed.add(doubleVal);
        }
        return parsed;
    }

    public static String buildSinkFilename(Dotenv config, boolean summ) {
        String name = "./newsinks/";
        name += config.get("DATASET") + 
                    "-d" + config.get("DIMENSIONS") + 
                    "-" + config.get("ALGORITHM") + 
                    "-k" + config.get("k") + 
                    "-" + config.get("DISTANCE_MEASURE").substring(0, 3) + 
                    "-" + config.get("ANNS");
        if (config.get("ANNS").equals("LSH")) {
            name += "-h" + config.get("HASHES") + 
                    "-t" + config.get("HASHTABLES");
        }
        if (summ) {
            name += "-w" + config.get("WINDOW") + 
                "-i" + config.get("INLIER_PERCENTAGE");
            name += "-a" + config.get("MAX_AGE");
        }
        return name.toLowerCase();
    }
    
}
