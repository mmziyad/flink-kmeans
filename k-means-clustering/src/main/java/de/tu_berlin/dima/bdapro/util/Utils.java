package de.tu_berlin.dima.bdapro.util;

import de.tu_berlin.dima.bdapro.datatype.ClusterCenter;
import de.tu_berlin.dima.bdapro.datatype.IndexedPoint;
import de.tu_berlin.dima.bdapro.datatype.Point;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RichReduceFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import java.io.*;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;

/**
 * Created by JML on 12/10/16.
 */
public class Utils {

    public static void mergeFile(String inputDir, String outputFile) throws Exception{
        File dir = new File(inputDir);
        if(dir.isDirectory()){
            File[] files = dir.listFiles();
            BufferedWriter outputWriter = new BufferedWriter(new FileWriter(new File(outputFile)));
            for(int i =0; i<files.length; i++){
                BufferedReader inputReader = new BufferedReader(new FileReader(files[i]));
                String line = inputReader.readLine();
                while (line != null){
                    outputWriter.write(line + "\n");
                    line = inputReader.readLine();
                }
                inputReader.close();
            }
            outputWriter.close();
        }
    }

    public static String vectorToCustomString(Point vector){
        StringBuilder result = new StringBuilder();
        for (int i = 0; i < vector.getFields().length; i++) {
            result.append(vector.getFields()[i]);
            result.append(" ");
        }
        return result.toString();
    }


    /**
     * Determines the closest cluster center for a data point.
     */
    @FunctionAnnotation.ForwardedFields("*->1")
    public static class SelectNearestCenter extends RichMapFunction<IndexedPoint, Tuple3<Integer, IndexedPoint, Long>> {
        private Collection<ClusterCenter> centroids;

        /**
         * Reads the centroid values from a broadcast variable into a collection.
         */
        @Override
        public void open(Configuration parameters) throws Exception {
            this.centroids = getRuntimeContext().getBroadcastVariable("centroids");
        }

        @Override
        public Tuple3<Integer, IndexedPoint, Long> map(IndexedPoint p) throws Exception {
            double minDistance = Double.MAX_VALUE;
            int closestCentroidId = -1;

            // check all cluster centers
            for (ClusterCenter centroid : centroids) {

                // compute distance, using SquaredEuclideanDistanceMetric
                // We need only the squared value for the comparison.
                double distance = p.getPoint().squaredDistance(centroid.getPoint());

                // update nearest cluster if necessary
                if (distance < minDistance) {
                    minDistance = distance;
                    closestCentroidId = centroid.getIndex();
                }
            }
            p.setIndex(closestCentroidId);
            // emit a new record with the center id, the data point, count =1
            // Tuple3<Integer, IndexedPoint, Long> -> (index, data, count=1)
            return new Tuple3<Integer, IndexedPoint, Long>(closestCentroidId, p, 1L);
        }
    }

    public static class SumClusterPointsReduceFunc extends RichReduceFunction<Tuple3<Integer, IndexedPoint, Long>>  {
        @Override
        public Tuple3<Integer, IndexedPoint, Long> reduce(Tuple3<Integer, IndexedPoint, Long> tuple, Tuple3<Integer, IndexedPoint, Long> t1) throws Exception {
            IndexedPoint temp = new IndexedPoint(tuple.f1.getIndex(), tuple.f1.getPoint().add(t1.f1.getPoint()));
            return new Tuple3<Integer, IndexedPoint, Long>(t1.f0, temp, t1.f2 + tuple.f2);
        }
    }

    public static class MapSumToClusterCenterFunc implements MapFunction<Tuple3<Integer, IndexedPoint, Long>, ClusterCenter>{
        @Override
        public ClusterCenter map(Tuple3<Integer, IndexedPoint, Long> tuple) throws Exception {
            Point centerPoint = tuple.f1.getPoint().divideByScalar(tuple.f2);
            ClusterCenter center = new ClusterCenter(tuple.f0, centerPoint, true);
            center.setRow(tuple.f2);
            return center;
        }
    }

}
