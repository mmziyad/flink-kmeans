package de.tu_berlin.dima.bdapro.datatype;

import de.tu_berlin.dima.bdapro.datatype.Point;

import java.io.Serializable;
import java.util.Arrays;

/**
 * Created by JML on 1/9/17.
 */
public class ClusterCenter extends IndexedPoint{
    private long row;
    private int sumDistance;
    private boolean isLeafNode;

    public ClusterCenter(int index, double[] fields) {
        this.index = index;
        this.fields = fields;
        this.row = 0;
        this.sumDistance = 0;
        this.isLeafNode = false;
    }

    public ClusterCenter(int index, double[] fields, boolean isLeafNode) {
        this.index = index;
        this.fields = fields;
        this.row = 0;
        this.sumDistance = 0;
        this.isLeafNode = isLeafNode;
    }

    public void addRow() {
        this.row++;
    }

    public int getIndex() {
        return index;
    }

    public void setIndex(int index) {
        this.index = index;
    }

    public long getRow() {
        return row;
    }

    public void setRow(long row) {
        this.row = row;
    }

    public int getSumDistance() {
        return sumDistance;
    }

    public void setSumDistance(int sumDistance) {
        this.sumDistance = sumDistance;
    }

    public boolean isLeafNode() {
        return isLeafNode;
    }

    public void setLeafNode(boolean leafNode) {
        isLeafNode = leafNode;
    }

    @Override
    public String toString() {
        return this.index + " - " + Arrays.toString(this.fields) + " - " + this.row + " - " + this.isLeafNode;
    }
}
