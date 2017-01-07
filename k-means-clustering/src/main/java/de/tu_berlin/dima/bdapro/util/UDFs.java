package de.tu_berlin.dima.bdapro.util;

import de.tu_berlin.dima.bdapro.datatype.Point;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import java.util.*;

/**
 * Created by zis on 02/01/17.
 */
public class UDFs {

    /**
     * Create Point data from the input record
     */
    public static class PointData implements MapFunction<String, Point> {
        private Point point;

        public PointData(int dimensions) {
            this.point = new Point(dimensions);
        }

        @Override
        public Point map(String value) {
            String fields[] = value.split(Constants.DELIMITER);
            for (int i = 0; i < fields.length; i++) {
                point.getFields()[i] = Double.parseDouble(fields[i]);
            }
            return point;
        }
    }

    /**
     * Label the cluster centres from 0 to (k-1)
     */
    public static class CentroidLabeler implements GroupReduceFunction<Point, Tuple2<Integer, Point>> {
        @Override
        public void reduce(Iterable<Point> iterable, Collector<Tuple2<Integer, Point>> collector) throws Exception {
            int label = 0;
            for (Point p : iterable) {
                collector.collect(new Tuple2<>(label++, p));
            }
        }
    }

    /**
     * Determines the closest cluster center for a data point.
     */
    public static final class SelectNearestCenter extends RichMapFunction<Point, Tuple2<Integer, Point>> {
        private Collection<Tuple2<Integer, Point>> centroids;

        // Reads the centroid values from a broadcast variable into a collection
        @Override
        public void open(Configuration parameters) throws Exception {
            this.centroids = getRuntimeContext().getBroadcastVariable("centroids");
        }

        @Override
        public Tuple2<Integer, Point> map(Point p) throws Exception {
            double minDistance = Double.MAX_VALUE;
            int closestCentroidId = -1;
            // check all cluster centers
            for (Tuple2<Integer, Point> centroid : centroids) {
                // compute distance, using SquaredEuclideanDistanceMetric
                // Note: We need only the squared value for the comparison.
                double distance = p.squaredDistance(centroid.f1);
                // update nearest cluster if necessary
                if (distance < minDistance) {
                    minDistance = distance;
                    closestCentroidId = centroid.f0;
                }
            }
            // emit a new record with the center id and the data point.
            return new Tuple2<>(closestCentroidId, p);
        }
    }

    /**
     * Appends a count variable to the tuple.
     */
    @FunctionAnnotation.ForwardedFields("f0;f1")
    public static final class CountAppender implements MapFunction<Tuple2<Integer, Point>, Tuple3<Integer, Point, Long>> {
        @Override
        public Tuple3<Integer, Point, Long> map(Tuple2<Integer, Point> t) {
            return new Tuple3<>(t.f0, t.f1, 1L);
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
    public static final class CentroidAverager implements MapFunction<Tuple3<Integer, Point, Long>, Tuple2<Integer, Point>> {
        @Override
        public Tuple2<Integer, Point> map(Tuple3<Integer, Point, Long> value) {
            return new Tuple2<>(value.f0, value.f1.divideByScalar(value.f2));
        }
    }

    /**
     * Check if the clusters are converged, based on the provided threshold
     */
    public static class ConvergenceEvaluator implements FlatMapFunction<Tuple2<Tuple2<Integer, Point>, Tuple2<Integer, Point>>, Tuple2<Integer, Point>> {

        private double threshold;

        public ConvergenceEvaluator(double threshold) {
            this.threshold = threshold;
        }

        @Override
        public void flatMap(Tuple2<Tuple2<Integer, Point>, Tuple2<Integer, Point>> val, Collector<Tuple2<Integer, Point>> collector) throws Exception {
            if (!evaluateConvergence(val.f0.f1, val.f1.f1, threshold)) {
                collector.collect(val.f0);
            }
        }

        private boolean evaluateConvergence(Point p1, Point p2, double threshold) {
            return (p1.squaredDistance(p2) <= threshold * threshold);
        }
    }

    /**
     * Implementation of local kMeans using kMeans++ initialization
     * Adapted from Apache Spark Implementation: LocalKMeans.scala
     */
    public static class LocalKMeans implements GroupReduceFunction<Tuple3<Integer, Point, Long>, Point> {
        private int k;
        private int maxIter;
        private Random random;
        private int dimensions;

        public LocalKMeans(int d, int k, int maxIter) {
            this.dimensions = d;
            this.k = k;
            this.maxIter = maxIter;
            this.random = new Random(Long.MAX_VALUE);
        }

        @Override
        public void reduce(Iterable<Tuple3<Integer, Point, Long>> iterable, Collector<Point> collector) throws Exception {
            // Identify initial k centres using kmeans ++
            List<Point> points = new ArrayList<>();
            List<Long> weights = new ArrayList<>();
            List<Double> costs = new ArrayList<>();

            Point centres[] = new Point[k];
            for (Tuple3 val : iterable) {
                points.add((Point) val.f1);
                weights.add((Long) val.f2);
            }

            Point initialCentre = getInitialCentre(points, weights);
            centres[0] = initialCentre;

            for (int i = 0; i < points.size(); i++) {
                costs.add(i, points.get(i).squaredDistance(centres[0]));
            }

            for (int i = 1; i < k; i++) {
                double sum = 0;
                for (int index = 0; index < points.size(); index++) {
                    sum += weights.get(index) * costs.get(index);
                }
                double r = random.nextDouble() * sum;
                int j = 0;
                double cumulativeScore = 0;
                while (j < points.size() && cumulativeScore < r) {
                    cumulativeScore += weights.get(j) * costs.get(j);
                    j += 1;
                }
                if (j == 0) {
                    centres[i] = points.get(0);
                } else {
                    centres[i] = points.get(j - 1);
                }
                for (int index = 0; index < points.size(); index++) {
                    costs.set(index, Math.min(points.get(index).squaredDistance(centres[i]), costs.get(index)));
                }
            }

            // Perform Lloyd's algorithm iterations
            for (int iteration = 0; iteration < maxIter; iteration++) {
                Map<Point, List<Point>> clusterMembers = new HashMap<>();
                for (Point centre : centres) {
                    clusterMembers.put(centre, new ArrayList<>());
                }
                for (Point point : points) {
                    double minDistance = Double.MAX_VALUE;
                    Point correctCentre = null;
                    for (Point centre : centres) {
                        double distance = point.squaredDistance(centre);
                        if (distance < minDistance) {
                            minDistance = distance;
                            correctCentre = centre;
                        }
                    }
                    clusterMembers.get(correctCentre).add(point);
                }


                for (int i = 0; i < k; i++) {
                    centres[i] = findCentroidOfPoints(clusterMembers.get(centres[i]), dimensions);

                }
            }
            // The updated centres
            for (Point centre : centres) {
                collector.collect(centre);
            }
        }

        /**
         * Find the centroid of given Points
         */
        private Point findCentroidOfPoints(List<Point> points, int dimensions) {
            Point centroid = new Point(dimensions);
            for (Point p : points) {
                centroid = centroid.add(p);
            }
            return centroid.divideByScalar(points.size());
        }

        /**
         * Get the initial centre from the weighted points
         */
        private Point getInitialCentre(List<Point> points, List<Long> weights) {
            long sumOfWeights = 0;
            for (long weight : weights) sumOfWeights += weight;
            double r = random.nextDouble() * sumOfWeights;
            int i = 0;
            double cureWeight = 0;
            while (i < points.size() && cureWeight < r) {
                cureWeight += weights.get(i);
                i += 1;
            }
            return points.get(i - 1);
        }
    }
}
