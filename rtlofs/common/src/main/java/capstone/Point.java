package capstone;

import java.util.ArrayList;

import be.tarsos.lsh.Vector;

public class Point {
    public ArrayList<Double> attributes;
    public int dim;
    public String key; // kafka identifier
    protected int hashCode;
    protected boolean cached = false;
    // TODO remove this, it's not object oriented enough (not related to the concept of a point and only sometimes relevant)
    public boolean lshHashed = false;

    protected Point() { }

    public Point(int d, ArrayList<Double> parsed) {
      this.dim = d;
      this.attributes = parsed;
    }

    public Point(String key, int d, ArrayList<Double> parsed) {
      this.key = key;
      this.dim = d;
      this.attributes = parsed;
    }

    public void setKey(String key) {
      this.key = key;
    }

    public Double getAttribute(int index) {
      return this.attributes.get(index);
    }

    public Double getDistanceTo(Point other, String distanceMeasure) {
      try {
        if (other.getClass().equals(VPoint.class)) {
          return ((VPoint)other).getDistanceTo(this, distanceMeasure);
        }
        double distance = 0;
        switch (distanceMeasure) {
          case "EUCLIDEAN":
            for (int i = 0; i < this.dim; i++) {
              distance += Math.pow(this.getAttribute(i) - other.getAttribute(i), 2);
            }
            // IMPROVE: maybe remove sqrt to speed things up a tiny bit
            // if that is done, make sure to square predefined vp distances too
            return Math.sqrt(distance);
          case "MANHATTAN":
            for (int i = 0; i < this.dim; i++) {
              distance += Math.abs(this.getAttribute(i) - other.getAttribute(i));
            }
            return distance;
          default:
            System.err.println("bad dist measure");
            return null;
        }
      } catch (Exception e) {
        System.out.println("super getDistanceTo " + e + " " + e.getStackTrace()[0].getLineNumber());
      }
      return null;
    }

    public Vector toVector() {
      // NOTE: this is NOT java.util.Vector, this is be.tarsos.lsh.Vector
      double[] arr = new double[this.dim];
      for (int i = 0; i < this.dim; i++) {
        arr[i] = this.getAttribute(i);
      }
      String key = this.key;
      boolean virtual = false;
      if (this.getClass().equals(VPoint.class)) {
        virtual = true;
        key = ((VPoint)this).center.toString();
      }
      return new Vector(key, arr, virtual);
    }

    public static Point fromVector(Vector v) {
      double[] arr = v.getValues();
      ArrayList<Double> attrs = new ArrayList<>();
      for (double x : arr) {
        attrs.add(x);
      }
      if (v.virtual) {
        return new VPoint(Utils.parse(v.getKey(), " ", attrs.size()), attrs);
      }
      return new Point(v.getKey(), arr.length, attrs);
    }

    @Override
    public boolean equals(Object other) {
      if (other == null || !(other.getClass().equals(Point.class))) return false;
      Point otherPoint = (Point)other;
      boolean mismatch = false;
      for (int i = 0; i < this.dim; i++) {
        mismatch = this.getAttribute(i).equals(otherPoint.getAttribute(i)) == false;
        if (mismatch) break;
      }
      return !mismatch;
    }

    @Override
    public int hashCode() {
      if (this.cached) return this.hashCode;
      String str = "";
      for (int i = 0; i < this.dim; i++) {
        str += this.getAttribute(i).toString();
      }
      int hc = str.hashCode();
      this.cached = true;
      this.hashCode = hc;
      return hc;
    }

    @Override
    public String toString() {
      String str = "";
      for (int i = 0; i < this.dim; i++) {
        str += this.getAttribute(i) + " ";
      }
      return str;
    }
}
