package de.tu_berlin.dima.bdapro.datatype;

/**
 * Created by zis on 12/01/17.
 */
public class Centroid extends Point {
    public int id;

    public Centroid() {
    }

    public int getId() {
        return id;
    }

    public Centroid(int id, double[] data) {
        super(data);
        this.id = id;
    }

    public Centroid(int id, Point p) {
        super(p.getFields());
        this.id = id;
    }

    @Override
    public String toString() {
        return id + " " + super.toString();
    }
}

