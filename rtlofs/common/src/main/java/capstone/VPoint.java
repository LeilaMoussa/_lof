package capstone;

import java.util.ArrayList;

public class VPoint extends Point {
    ArrayList<Double> attributes;
    int dim;
    public double R;
    Point center;
    int hplane; // hyperplane
    int position; // left or right for example

    public VPoint(Point center, double radius, int d, int hplane, int position) {
        this.R = radius;
        this.center = center;
        this.dim = d;
        this.hplane = hplane;
        this.position = position;
        this.attributes = new ArrayList<>(center.attributes);
        // hplane is in [0, d[
        final double centerCoord = center.getAttribute(hplane);
        switch (position) {
            case 0: this.attributes.set(hplane, centerCoord + R); break;
            case 1: this.attributes.set(hplane, centerCoord - R); break;
            default: System.out.println("VP position can either be 0 or 1.");
        }
        
    }

    @Override
    public boolean equals(Object other) {
      if (other == null || !(other instanceof VPoint)) return false;
      VPoint otherPoint = (VPoint)other;
      return otherPoint.attributes.equals(this.attributes);
    }

    // TODO: hashCode

    @Override
    public String toString() {
      return "VP (" + this.center + ", " + this.hplane + " , " + this.position + ")";
    }
}
