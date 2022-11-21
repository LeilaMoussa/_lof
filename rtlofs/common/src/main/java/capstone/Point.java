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
      return index < this.dim ? this.attributes.get(index) : null;
    }

    public Double getDistanceTo(Point other, String distanceMeasure) {
      double distance = 0;
      switch (distanceMeasure) {
        case "EUCLIDEAN":
          for (int i = 0; i < this.dim; i++) {
            distance += Math.pow(this.getAttribute(i) - other.getAttribute(i), 2);
          }
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
    }

    public Vector toVector() {
      // NOTE: this is NOT java.util.Vector, this is be.tarsos.lsh.Vector
      // TODO: make this look nicer
      double[] arr = new double[this.dim];
      for (int i = 0; i < this.dim; i++) {
        arr[i] = this.getAttribute(i);
      }
      return new Vector("", arr); // null key
    }

    @Override
    public boolean equals(Object other) {
      if (other == null || !(other instanceof Point)) return false;
      Point otherPoint = (Point)other;
      boolean mismatch = false;
      for (int i = 0; i < this.dim; i++) {
        mismatch = this.getAttribute(i) != otherPoint.getAttribute(i);
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
