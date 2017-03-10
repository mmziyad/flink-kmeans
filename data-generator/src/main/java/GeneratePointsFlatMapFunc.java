import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import java.util.List;
import java.util.Random;

/**
 * Created by JML on 2/6/17.
 */
public class GeneratePointsFlatMapFunc extends RichFlatMapFunction<Tuple4<Integer, Long, Double, Random>, Point> {

    private List<Point> centroids;

    /**
     * Reads the centroid values from a broadcast variable into a collection.
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        centroids = getRuntimeContext().getBroadcastVariable("centroids");
        /*for(int i =0; i<centroids.size(); i++){
            System.out.println(centroids.get(i));
        }*/
    }

    @Override
    public void flatMap(Tuple4<Integer, Long, Double, Random> partitionInfo, Collector<Point> collector) throws Exception {
            Random rnd = partitionInfo.f3;
            Double stddev = partitionInfo.f2;
            Long nbPoints = partitionInfo.f1;
            int nextCentroid = 0;
            for (int i = 1; i <= nbPoints; i++) {
                // generate a point for the current centroid
                Point centroid = centroids.get(nextCentroid);
                int dimension = centroid.getFields().length;
                Point point = new Point(dimension);
                for (int d = 0; d < dimension; d++) {
                    point.getFields()[d] = ((long)(Math.floor(rnd.nextGaussian() * stddev))) + centroid.getFields()[d];
                }
//                point.getFields()[dimension] = nextCentroid;
                collector.collect(point);
                nextCentroid = (nextCentroid + 1) % centroids.size();
            }
    }
}
