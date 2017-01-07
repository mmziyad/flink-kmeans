package de.tu_berlin.dima.bdapro.datatype;

import java.io.Serializable;
import java.util.Arrays;

/**
 * Created by zis on 06/01/17.
 */
public class Point implements Serializable {

    private int dimensions;
    private double fields[];

    public Point(int dimensions) {
        this.dimensions = dimensions;
        this.fields = new double[dimensions];
    }

    public double[] getFields() {
        return fields;
    }

    public void setFields(double[] fields) {
        this.fields = fields;
    }

    public double squaredDistance(Point other) {
        double distance = 0;
        for (int i = 0; i < dimensions; i++) {
            distance += (fields[i] - other.fields[i]) * (fields[i] - other.fields[i]);
        }
        return distance;
    }

    public Point add (Point other){
        for (int i = 0; i < dimensions; i++) {
            fields[i] += other.fields[i];
        }
        return this;
    }

    public Point divideByScalar (long val){
        for (int i = 0; i < dimensions; i++) {
            fields[i] /= val;
        }
        return this;
    }

    @Override
    public String toString() {
        return Arrays.toString(fields);
    }
}
