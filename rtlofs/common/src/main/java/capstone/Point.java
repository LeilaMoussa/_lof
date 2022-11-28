package capstone;

import java.util.ArrayList;

import be.tarsos.lsh.Vector;

public class Point {
    ArrayList<Double> attributes;
    int dim;

    protected Point() { }

    public Point(int d, ArrayList<Double> parsed) {
      this.dim = d;
      this.attributes = parsed;
    }

    public Double getAttribute(int index) {
      return this.attributes.get(index);
    }

    public Double getDistanceTo(Point other, String distanceMeasure) {
      try {
        double distance = 0;
        switch (distanceMeasure) {
          case "EUCLIDEAN":
            for (int i = 0; i < this.dim; i++) {
              distance += Math.pow(this.getAttribute(i) - other.getAttribute(i), 2);
            }
            double x = Math.sqrt(distance);
            // if (other.getClass().equals(VPoint.class)) {
            //   VPoint v = (VPoint)other;
            //   if (x != (v.dim + v.R) && x != (v.dim - v.R) && x != Math.sqrt(Math.pow(v.dim, 2) + Math.pow(v.R, 2))) {
            //     System.out.println("unexpected distance to vp");
            //   }
            // }
            return x;
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
        System.out.println("getDistanceTo " + e + " " + e.getStackTrace()[0].getLineNumber());
      }
      return null;
    }

    public Vector toVector() {
      // NOTE: this is NOT java.util.Vector, this is be.tarsos.lsh.Vector
      double[] arr = new double[this.dim];
      for (int i = 0; i < this.dim; i++) {
        arr[i] = this.getAttribute(i);
      }
      String key = null;
      if (this.getClass().equals(VPoint.class)) {
        key = ((VPoint)this).center.toString();
      };
      return new Vector(key, arr);
    }

    public static Point fromVector(Vector v) {
      double[] arr = v.getValues();
      ArrayList<Double> attrs = new ArrayList<>();
      for (double x : arr) {
        attrs.add(x);
      }
      if (v.getKey() != null) {
        return new VPoint(Utils.parse(v.getKey(), " ", attrs.size()), attrs);
      }
      return new Point(arr.length, attrs);
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
      String str = "";
      for (int i = 0; i < this.dim; i++) {
        str += this.getAttribute(i).toString();
      }
      return str.hashCode();
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
