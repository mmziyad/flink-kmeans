import java.io.Serializable;
import java.util.Arrays;


/**
 * Created by zis on 06/01/17.
 */
public class Point implements Serializable {

    private double fields[];

    public Point() {}

    public Point(int dimensions) {
        this.fields = new double[dimensions];
    }

    public Point(double fields[]) {
        this.fields = fields;
    }

    public double[] getFields() {
        return fields;
    }

    @Override
    public String toString() {
        StringBuilder result = new StringBuilder();
        for (int i = 0; i < fields.length; i++) {
            result.append(fields[i]);
            result.append(" ");
        }
        return result.toString();

    }
}
