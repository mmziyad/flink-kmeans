package de.tu_berlin.dima.bdapro.util;

import de.tu_berlin.dima.bdapro.datatype.ClusterCenter;
import de.tu_berlin.dima.bdapro.datatype.IndexedPoint;
import de.tu_berlin.dima.bdapro.datatype.Point;
import org.apache.flink.api.common.functions.*;
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


    //    /**
//     * Determines the closest cluster center for a data point.
//     */
//    @FunctionAnnotation.ForwardedFields("*->1")
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
                double distance = p.squaredDistance(centroid);

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
            IndexedPoint temp = tuple.f1.add(t1.f1);
            return new Tuple3<Integer, IndexedPoint, Long>(t1.f0, temp, t1.f2 + tuple.f2);
        }
    }

    public static class MapSumToClusterCenterFunc implements MapFunction<Tuple3<Integer, IndexedPoint, Long>, ClusterCenter>{

        @Override
        public ClusterCenter map(Tuple3<Integer, IndexedPoint, Long> tuple) throws Exception {
            IndexedPoint centerPoint = tuple.f1.divideByScalar(tuple.f2);
            ClusterCenter center = new ClusterCenter(tuple.f0, centerPoint.getFields(), true);
            center.setRow(tuple.f2);
            return center;
        }
    }


    public static class SelectNearestCenter2 extends RichMapFunction<IndexedPoint, IndexedPoint> {
        private Collection<ClusterCenter> centroids;

        /**
         * Reads the centroid values from a broadcast variable into a collection.
         */
        @Override
        public void open(Configuration parameters) throws Exception {
            this.centroids = getRuntimeContext().getBroadcastVariable("centroids2");
        }

        @Override
        public IndexedPoint map(IndexedPoint p) throws Exception {
            double minDistance = Double.MAX_VALUE;
            int closestCentroidId = -1;

            // check all cluster centers
            for (ClusterCenter centroid : centroids) {

                // compute distance, using SquaredEuclideanDistanceMetric
                // We need only the squared value for the comparison.
                double distance = p.squaredDistance(centroid);

                // update nearest cluster if necessary
                if (distance < minDistance) {
                    minDistance = distance;
                    closestCentroidId = centroid.getIndex();
                }
            }
            p.setIndex(closestCentroidId);
            // emit a new record with the center id, the data point, count =1
            // Tuple3<Integer, IndexedPoint, Long> -> (index, data, count=1)
            return p;
        }
    }

    public static class UpdateRootForSplittedCentersMapFunc implements MapFunction<ClusterCenter, Tuple3<Integer, Long, Boolean>>{
        ClusterCenter splittedCenter;

        public UpdateRootForSplittedCentersMapFunc(ClusterCenter splittedCenter){
            this.splittedCenter = splittedCenter;
        }

        @Override
        public Tuple3<Integer, Long, Boolean> map(ClusterCenter clusterCenter) throws Exception {
            if(splittedCenter.getIndex() == clusterCenter.getIndex()){
                clusterCenter.setLeafNode(false);
            }
            return new Tuple3<Integer, Long, Boolean>(clusterCenter.getIndex(), clusterCenter.getRow(), clusterCenter.isLeafNode());
        }
    }


    public static class FilterDividableDataFunc implements FilterFunction<IndexedPoint>{
        int centerIndex;
        public FilterDividableDataFunc(int centerIndex){
            this.centerIndex = centerIndex;
        }
        @Override
        public boolean filter(IndexedPoint point) throws Exception {
            return point.getIndex() == centerIndex;
        }
    }

    public static class FilterUndividableDataFunc implements FilterFunction<IndexedPoint>{
        int centerIndex;

        public FilterUndividableDataFunc(int centerIndex){
            this.centerIndex = centerIndex;
        }
        @Override
        public boolean filter(IndexedPoint point) throws Exception {
            return point.getIndex() != centerIndex;
        }
    }


    /**
     * Create Point data from the input record
     */
    public static class DefaultIndexedPointData implements MapFunction<String, IndexedPoint> {
        @Override
        public IndexedPoint map(String value) {
            String cols[] = value.split(Constants.IN_DELIMITER);
            double fields[] = new double[cols.length];
            for (int i = 0; i < fields.length; i++) {
                fields[i] = Double.parseDouble(cols[i]);
            }
            return new IndexedPoint(1, fields);
        }
    }

}
