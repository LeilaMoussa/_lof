package capstone;

import java.util.ArrayList;

import be.tarsos.lsh.Vector;

public class VPoint extends Point {
    final ArrayList<Double> attributes;
    final int dim;
    final Position pos;
    public double R;
    final Point center;

    public VPoint(Position pos, double radius, Point center) {
        this.attributes = new ArrayList<>(0);
        this.dim = 0;
        this.pos = pos;
        this.R = radius;
        this.center = center;
    }

    // might need this:
    @Override
    public Vector toVector() {
        return new Vector("", new double[0]);
    }

    @Override
    public Double getDistanceTo(Point other, String distanceMeasure) {
        double d = other.getDistanceTo(center, distanceMeasure);
        switch (this.pos) {
            case RIGHT: return d-R;
            case LEFT: return d+R;
            case TOPBOTTOM:
                if (distanceMeasure.equals("EUCLIDEAN")) {
                    return Math.sqrt(Math.pow(d, 2) + Math.pow(R, 2));
                } else if (distanceMeasure.equals("MANHATTAN")) {
                    return d+R;
                }
        }
        return null;
    }

    @Override
    public boolean equals(Object other) {
      if (other == null || !(other instanceof VPoint)) return false;
      VPoint otherPoint = (VPoint)other;
      // TODO think about this second condition.
      return otherPoint.center.equals(this.center) && otherPoint.pos == this.pos;
    }

    // TODO: hashCode?

    @Override
    public String toString() {
      return "VPoint for center " + this.center;
    }
}
