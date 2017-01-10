package de.tu_berlin.dima.bdapro.datatype;

import java.io.Serializable;

/**
 * Created by JML on 1/9/17.
 */
public class IndexedPoint implements Serializable {
    private int index;
    private Point point;

    public IndexedPoint(Point point){
        this.point = point;
    }

    public IndexedPoint(int index, Point point){
        this.index = index;
        this.point = point;
    }

    public int getIndex() {
        return index;
    }

    public void setIndex(int index) {
        this.index = index;
    }

    public Point getPoint() {
        return point;
    }

    public void setPoint(Point point) {
        this.point = point;
    }

    @Override
    public String toString() {
        return this.index + " - " + this.point.toString();
    }
}
