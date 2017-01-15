package de.tu_berlin.dima.bdapro.util;

import de.tu_berlin.dima.bdapro.datatype.Centroid;
import de.tu_berlin.dima.bdapro.datatype.Point;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import java.util.Collection;
import java.util.Random;

/**
 * Created by zis on 02/01/17.
 */
public class UDFs {

    /**
     * Create Point data from the input record
     */
    public static class PointData implements MapFunction<String, Point> {
        double[] data;

        public PointData(int dimensions) {
            data = new double[dimensions];
        }

        @Override
        public Point map(String value) {
            String fields[] = value.split(Constants.IN_DELIMITER);
            for (int i = 0; i < fields.length; i++) {
                data[i] = Double.parseDouble(fields[i]);
            }
            return new Point(data);
        }
    }

    /**
     * Label the cluster centres from 0 to (k-1)
     */
    public static class CentroidLabeler implements GroupReduceFunction<Point, Centroid> {
        @Override
        public void reduce(Iterable<Point> values, Collector<Centroid> out) throws Exception {
            int label = 0;
            for (Point p : values) {
                out.collect(new Centroid(label++, p));
            }
        }
    }

    /**
     * Determines the closest cluster center for a data point.
     */
    public static final class SelectNearestCenter extends RichMapFunction<Point, Tuple2<Integer, Point>> {
        private Collection<Centroid> centroids;

        /**
         * Reads the centroid values from a broadcast variable into a collection.
         */
        @Override
        public void open(Configuration parameters) throws Exception {
            this.centroids = getRuntimeContext().getBroadcastVariable("centroids");
        }

        @Override
        public Tuple2<Integer, Point> map(Point p) throws Exception {

            double minDistance = Double.MAX_VALUE;
            int closestCentroidId = -1;

            // check all cluster centers
            for (Centroid centroid : centroids) {
                // compute distance
                double distance = p.squaredDistance(centroid);

                // update nearest cluster if necessary
                if (distance < minDistance) {
                    minDistance = distance;
                    closestCentroidId = centroid.id;
                }
            }

            // emit a new record with the center id and the data point.
            return new Tuple2<>(closestCentroidId, p);
        }
    }

    /**
     * Appends a count variable to the tuple.
     */
    public static final class CountAppender implements MapFunction<Tuple2<Integer, Point>, Tuple3<Integer, Point, Long>> {

        @Override
        public Tuple3<Integer, Point, Long> map(Tuple2<Integer, Point> t) {
            return new Tuple3<>(t.f0, t.f1, 1L);
        }
    }

    /**
     * Sums and counts point coordinates.
     */
    public static final class CentroidAccumulator implements ReduceFunction<Tuple3<Integer, Point, Long>> {

        @Override
        public Tuple3<Integer, Point, Long> reduce(Tuple3<Integer, Point, Long> val1, Tuple3<Integer, Point, Long> val2) {
            return new Tuple3<>(val1.f0, val1.f1.add(val2.f1), val1.f2 + val2.f2);
        }
    }

    /**
     * Computes new centroid from coordinate sum and count of points.
     */
    public static final class CentroidAverager implements MapFunction<Tuple3<Integer, Point, Long>, Centroid> {

        @Override
        public Centroid map(Tuple3<Integer, Point, Long> value) {
            return new Centroid(value.f0, value.f1.divideByScalar(value.f2));
        }
    }

    /**
     * Finds the closest centroid, and calculate the sq. distance to the closest centre as cost
     */
    public static class CostFinder extends RichMapFunction<Point, Tuple2<Point, Double>> {

        private Collection<Point> centroids;

        // Reads the centroid values from a broadcast variable into a collection
        @Override
        public void open(Configuration parameters) throws Exception {
            this.centroids = getRuntimeContext().getBroadcastVariable("centroids");
        }

        @Override
        public Tuple2<Point, Double> map(Point p) throws Exception {
            double minDistance = Double.MAX_VALUE;

            // check all cluster centers
            for (Point centroid : centroids) {
                // compute distance, using SquaredEuclideanDistanceMetric
                // Note: We need only the squared value for the comparison.
                double distance = p.squaredDistance(centroid);
                // update nearest cluster if necessary
                if (distance < minDistance) {
                    minDistance = distance;
                }
            }
            // emit a new record with the data point and calculated cost
            return new Tuple2<>(p, minDistance);
        }
    }

    /**
     * Retain only the points which fits into the given probability distribution
     */

    public static class ProbabilitySamplingFilter extends RichFilterFunction<Tuple2<Point, Double>> {

        private double sumCosts;
        private int overSamplingFactor;
        private Random random;

        public ProbabilitySamplingFilter(int l, long seed) {
            this.overSamplingFactor = l;
            this.random = new Random(seed);
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            Collection<Tuple2<Point, Double>> aggregatePointCosts = getRuntimeContext().getBroadcastVariable("sumCosts");
            for (Tuple2<Point, Double> entry : aggregatePointCosts) {
                sumCosts += entry.f1;
            }
        }

        @Override
        public boolean filter(Tuple2<Point, Double> pointCost) throws Exception {
            return this.random.nextDouble() < (this.overSamplingFactor * pointCost.f1 / sumCosts);
        }
    }

    /**
     * Check if the clusters are converged, based on the provided threshold
     */
    public static class ConvergenceEvaluator implements FlatMapFunction<Tuple2<Centroid, Centroid>, Centroid> {

        private double threshold;

        public ConvergenceEvaluator(double threshold) {
            this.threshold = threshold;
        }

        @Override
        public void flatMap(Tuple2<Centroid, Centroid> val, Collector<Centroid> collector) throws Exception {
            if (!evaluateConvergence(val.f0, val.f1, threshold)) {
                collector.collect(val.f0);
            }
        }
        private boolean evaluateConvergence(Point p1, Point p2, double threshold) {
            return (p1.squaredDistance(p2) <= threshold * threshold);
        }
    }

    /**
     * Read the KMeans output data for calculating Davies Bouldin Index
     */
    public static class DaviesBouldinIndexInput implements MapFunction<String, Tuple2<Integer, Point>> {

        double[] data;
        public DaviesBouldinIndexInput(int d) {
            data = new double[d];
        }

        @Override
        public Tuple2<Integer, Point> map(String value) throws Exception {
            String fields[] = value.split(Constants.OUT_DELIMITER);
            String point [] = fields[1].substring( 1, fields[1].length() - 1).split(", ");
            for (int i = 0; i < point.length; i++) {
                data[i] = Double.parseDouble(point[i]);
            }
            return new Tuple2<>(Integer.parseInt(fields[0]), new Point(data));
        }
    }
    /**
     * Find the distance of each cluster member from cluster centre
     */
    public static class intraClusterDistance extends RichMapFunction<Tuple3<Integer, Point, Long>, Tuple4<Integer, Point, Long, Double>> {

        private Collection<Centroid> centroids;
        @Override
        public void open(Configuration parameters) throws Exception {
            this.centroids = getRuntimeContext().getBroadcastVariable("centroids");
        }
        @Override
        public Tuple4<Integer, Point, Long ,Double> map(Tuple3<Integer, Point, Long> value) throws Exception {
            for (Centroid c: centroids){
                if (c.getId() == value.f0){
                    return new Tuple4<>(value.f0 , value.f1 , value.f2 , c.euclideanDistance(value.f1));
                }
            }
            return null;
        }
    }

    public static class DaviesBouldinIndexInputV1 implements MapFunction<String, Tuple2<Integer, Point>> {

        double[] data;
        public DaviesBouldinIndexInputV1(int d) {
            data = new double[d];
        }

        @Override
        public Tuple2<Integer, Point> map(String value) throws Exception {
            String fields[] = value.split(Constants.IN_DELIMITER);
            for (int i = 1; i < fields.length; i++) {
                data[i-1] = Double.parseDouble(fields[i]);
            }
            return new Tuple2<>(Integer.parseInt(fields[0]), new Point(data));
        }
    }
}
