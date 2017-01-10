package de.tu_berlin.dima.bdapro.datatype;

import de.tu_berlin.dima.bdapro.datatype.Point;

import java.io.Serializable;

/**
 * Created by JML on 1/9/17.
 */
public class ClusterCenter implements Serializable {
    private Point point;
    private int index;
    private long row;
    private int sumDistance;
    private boolean isLeafNode;

    public ClusterCenter(int index, Point point) {
        this.index = index;
        this.point = point;
        this.row = 0;
        this.sumDistance = 0;
        this.isLeafNode = false;
    }

    public ClusterCenter(int index, Point point, boolean isLeafNode) {
        this.index = index;
        this.point = point;
        this.row = 0;
        this.sumDistance = 0;
        this.isLeafNode = isLeafNode;
    }

    public void addRow() {
        this.row++;
    }

    public Point getPoint() {
        return point;
    }

    public void setPoint(Point point) {
        this.point = point;
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
        return this.index + " - " + this.point.toString() + " - " + this.row + " - " + this.isLeafNode;
    }
}
