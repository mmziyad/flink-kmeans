package de.tu_berlin.dima.bdapro.datatype;

import java.io.Serializable;
import java.util.Arrays;

/**
 * Created by JML on 1/9/17.
 */
public class IndexedPoint implements Serializable {
    protected int index;
    protected double fields[];

    public IndexedPoint(){

    }

    public IndexedPoint(int index, Point point){
        this.index = index;
        this.fields = point.getFields();
    }

    public IndexedPoint(int index, double fields[]) {
        this.index = index;
        this.fields = fields;
    }

    public double[] getFields() {
        return fields;
    }

    public void setFields(double[] fields) {
        this.fields = fields;
    }

    public double squaredDistance(IndexedPoint other) {
        double distance = 0;
        for (int i = 0; i < fields.length; i++) {
            distance += (fields[i] - other.fields[i]) * (fields[i] - other.fields[i]);
        }
        return distance;
    }

    public IndexedPoint add (IndexedPoint other){
        for (int i = 0; i < fields.length; i++) {
            fields[i] += other.fields[i];
        }
        return this;
    }

    public IndexedPoint divideByScalar (long val){
        for (int i = 0; i < fields.length; i++) {
            fields[i] /= val;
        }
        return this;
    }


    public int getIndex() {
        return index;
    }

    public void setIndex(int index) {
        this.index = index;
    }


    @Override
    public String toString() {
        return this.index + " - " + Arrays.toString(this.fields);
    }
}
