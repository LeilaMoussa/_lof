package capstone;

import java.util.ArrayList;

import be.tarsos.lsh.Vector;

public class VPoint extends Point {
    ArrayList<Double> attributes;
    int dim;
    //Position pos;
    public double R;
    final Point center;

    // public VPoint(Position pos, Point center, double radius) {
    //     this.attributes = new ArrayList<>(0);
    //     this.dim = 0;
    //     this.pos = pos;
    //     this.R = radius;
    //     this.center = center;
    // }

    public VPoint(Point center, double radius, int d, int hplane, int position) {
        this.R = radius;
        this.center = center;
        this.dim = d;
        this.attributes = new ArrayList<>(center.attributes);
        // hplane is in [0, d[
        final double centerCoord = center.getAttribute(hplane);
        switch (position) {
            case 0: this.attributes.set(hplane, centerCoord + R); break;
            case 1: this.attributes.set(hplane, centerCoord - R); break;
            default: System.out.println("VP position can either be 0 or 1.");
        }
        
    }

    // might need this:
    // TODO remove this, keep super impl
    @Override
    public Vector toVector() {
        return new Vector("", new double[0]);
    }

    // TODO make sure this account for new logic
    // alternative 1: fully rely on assumption that all but 2 points are sqrt(d²+r²) away
    // in that case, keep a count (internally or externally) st i <= 2 => d+R and d-R and i > 2 => srqt...
    // alternative 2: just instantiate that number of VPoints, each with d attributes derived from center
    // each dimension produces two points +/-R along that dimension
    // i kind of prefer the latter (while confirming that the distance calculations are what we expect)
    // @Override
    // public Double getDistanceTo(Point other, String distanceMeasure) {
    //     double d = other.getDistanceTo(center, distanceMeasure);
    //     switch (this.pos) {
    //         case RIGHT: return d-R;
    //         case LEFT: return d+R;
    //         case TOP:
    //         case BOTTOM:
    //             if (distanceMeasure.equals("EUCLIDEAN")) {
    //                 return Math.sqrt(Math.pow(d, 2) + Math.pow(R, 2));
    //             } else if (distanceMeasure.equals("MANHATTAN")) {
    //                 return d+R;
    //             } else {
    //                 System.out.println("Unsupported distance measure");
    //             }
    //     }
    //     return null;
    // }

    @Override
    public boolean equals(Object other) {
      if (other == null || !(other instanceof VPoint)) return false;
      VPoint otherPoint = (VPoint)other;
      return otherPoint.attributes.equals(this.attributes);
    }

    // TODO: hashCode

    @Override
    public String toString() {
      return "VPoint for center " + this.center;
    }
}
