package capstone;

import java.util.ArrayList;

public class VPoint extends Point {
    private double R;
    public Point center;
    // TODO: rename these fields
    private int hplane; // hyperplane, really represents dimensions
    private int position; // 0, 1, represents one of the 2 intersections between the axis and the hypersphere
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
    public Double getDistanceTo(Point other, String distanceMeasure) {
        try {
            assert(other.getClass().equals(Point.class));
            // pretend that other is oriented to be aligned with this group of vps
            Double d = this.center.getDistanceTo(other, distanceMeasure);
            if (this.hplane == 0) {
                if (this.position == 0) {
                    return d - R;
                } else if (this.position == 1) {
                    return d + R;
                } else {
                    System.out.println("There's been a problem setting vp position " + this.position);
                }
            } else {
                switch(distanceMeasure) {
                    case "EUCLIDEAN":
                        // TODO if getting rid of sqrt, do it everywhere
                        return Math.sqrt(
                            Math.pow(d, 2) +
                            Math.pow(R, 2)
                        );
                    case "MANHATTAN":
                        return d + R;
                    default: System.out.println("bad distance measure at vp getdistance");
                }
            }
        } catch (Exception e) {
            System.out.println("vp getDistanceTo " + e + " " + e.getStackTrace()[0].getLineNumber());
        }
        return null;
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
      return "VP( " + this.attributes + " )";
    }
}
