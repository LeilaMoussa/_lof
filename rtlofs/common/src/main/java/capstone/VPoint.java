package capstone;

import java.util.ArrayList;

public class VPoint extends Point {
    private double R;
    public Point center;
    private int hplane; // hyperplane
    private int position; // left or right for example
    // key is null, as the only use of a key is to output labels, which VPs don't have

    public VPoint(Point center, ArrayList<Double> attributes) {
        this.center = center;
        this.attributes = attributes;
        this.dim = attributes.size();
    }

    public VPoint(Point center, double radius, int d, int hplane, int position) {
        try {
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
        } catch (Exception e) {
            System.out.println("VPoint constructor " + e + " " + e.getStackTrace()[0].getLineNumber());
        }
    }

    @Override
    public boolean equals(Object other) {
      if (other == null || !(other.getClass().equals(VPoint.class))) return false;
      VPoint otherPoint = (VPoint)other;
      return otherPoint.attributes.equals(this.attributes);
    }

    @Override
    public int hashCode() {
        if (this.cached) return this.hashCode;
        int hc = this.center.hashCode() + this.hplane + this.position;
        this.cached = true;
        this.hashCode = hc;
        return hc;
    }

    @Override
    public String toString() {
      //return "VP (" + this.center + ", " + this.hplane + " , " + this.position + ")";
      return "VP( " + this.attributes + " )";
    }
}
