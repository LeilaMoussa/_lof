package capstone;

import java.util.ArrayList;

public class Point {
    // remove these, completely refactor to generic vector
    double x;
    double y;

    ArrayList<Double> attributes;
    int dim;

    Point(double x, double y) {
      this.x = x;
      this.y = y;
    }

    Point(int d, ArrayList<Double> parsed) {
      this.dim = d;
      this.attributes = parsed;
    }

    public Double getAttribute(int index) {
      return index < this.dim ? this.attributes.get(index) : null;
    }

    public double getDistanceTo(Point other) {
      // configable
      return Math.sqrt(Math.pow(this.x - other.x, 2) + Math.pow(this.y - other.y, 2));
    }

    public Double getGenericDistanceTo(Point other, String distanceMeasure) {
      double distance = 0;
      switch (distanceMeasure) {
        case "EUCLIDEAN":
          for (int i = 0; i < this.dim; i++) {
            distance += Math.pow(this.getAttribute(i) - other.getAttribute(i), 2);
          }
          return Math.sqrt(distance);
        case "MANHATTAN":
          for (int i = 0; i < this.dim; i++) {
            distance += this.getAttribute(i) - other.getAttribute(i);
          }
          return distance;
        default:
          return null;
      }
    }

    @Override
    public boolean equals(Object other) {
      return other != null && other instanceof Point &&
          ((Point) other).x == this.x && ((Point) other).y == this.y;
    }

    public boolean eq(Object other) {
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
      return (new Double(this.x).toString() + new Double(this.y).toString()).hashCode();
    }

    public int hash() {
      String str = "";
      for (int i = 0; i < this.dim; i++) {
        str += this.getAttribute(i).toString();
      }
      return str.hashCode();
    }

    @Override
    public String toString() {
      return this.x + " " + this.y;
    }

    public String tostring() {
      String str = "";
      for (int i = 0; i < this.dim; i++) {
        str += this.getAttribute(i) + " ";
      }
      return str;
    }
}
