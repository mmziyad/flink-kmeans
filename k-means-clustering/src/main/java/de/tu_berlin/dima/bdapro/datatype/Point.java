package de.tu_berlin.dima.bdapro.datatype;

import java.io.Serializable;
import java.util.Arrays;

/**
 * Created by zis on 06/01/17.
 */
public class Point implements Serializable {

    private double fields[];

    public Point(int dimensions) {
        this.fields = new double[dimensions];
    }

    public Point(double fields[]) {
        this.fields = fields;
    }

    public double[] getFields() {
        return fields;
    }

    public void setFields(double[] fields) {
        this.fields = fields;
    }

    public double squaredDistance(Point other) {
        double distance = 0;
        for (int i = 0; i < fields.length; i++) {
            distance += (fields[i] - other.fields[i]) * (fields[i] - other.fields[i]);
        }
        return distance;
    }

    public Point add (Point other){
        double data [] = fields.clone();
        for (int i = 0; i < fields.length; i++) {
            data[i] =  fields[i] + other.fields[i];
        }
        return new Point(data);
    }

    public Point divideByScalar (long val){
        double data [] = fields.clone();
        for (int i = 0; i < fields.length; i++) {
            data[i]  = fields[i]/val;
        }
        return new Point(data);
    }

    @Override
    public String toString() {
        return Arrays.toString(fields);
    }
}
