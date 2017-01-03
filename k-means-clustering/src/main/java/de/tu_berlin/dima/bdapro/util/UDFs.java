package de.tu_berlin.dima.bdapro.util;

import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.ml.math.DenseVector;
import org.apache.flink.ml.math.Vector;
import org.apache.flink.ml.metrics.distances.SquaredEuclideanDistanceMetric;
import org.apache.flink.util.Collector;

import java.util.*;

/**
 * Created by zis on 02/01/17.
 */
public class UDFs {

    /**
     * Create vector data from the input record
     */

    public static class VectorizedData implements MapFunction<String, Vector> {
        @Override
        public Vector map(String value) {
            String fields[] = value.split(Constants.DELIMITER);
            double[] fieldVals = new double[fields.length];
            for (int i = 0; i < fields.length; i++) {
                fieldVals[i] = Double.parseDouble(fields[i]);
            }
            Vector point = new DenseVector(fieldVals);
            return point;
        }
    }

    /**
     * Label the cluster centres from 0 to (k-1)
     */
    public static class CentroidLabeler implements GroupReduceFunction<Vector, Tuple2<Integer, Vector>> {
        @Override
        public void reduce(Iterable<Vector> iterable, Collector<Tuple2<Integer, Vector>> collector) throws Exception {
            int label = 0;
            for (Vector v : iterable) {
                collector.collect(new Tuple2<>(label++, v));
            }
        }
    }

    /**
     * Determines the closest cluster center for a data point.
     */
    public static final class SelectNearestCenter extends RichMapFunction<Vector, Tuple3<Integer, Vector, Vector>> {
        private Collection<Tuple2<Integer, Vector>> centroids;

        // Reads the centroid values from a broadcast variable into a collection
        @Override
        public void open(Configuration parameters) throws Exception {
            this.centroids = getRuntimeContext().getBroadcastVariable("centroids");
        }
        @Override
        public Tuple3<Integer, Vector, Vector> map(Vector p) throws Exception {
            double minDistance = Double.MAX_VALUE;
            SquaredEuclideanDistanceMetric squaredEuclideanDistanceMetric = new SquaredEuclideanDistanceMetric();
            int closestCentroidId = -1;
            Vector closestCendroid = null;
            // check all cluster centers
            for (Tuple2<Integer, Vector> centroid : centroids) {
                // compute distance, using SquaredEuclideanDistanceMetric
                // Note: We need only the squared value for the comparison.
                double distance = squaredEuclideanDistanceMetric.distance(p, centroid.f1);
                // update nearest cluster if necessary
                if (distance < minDistance) {
                    minDistance = distance;
                    closestCentroidId = centroid.f0;
                    closestCendroid = centroid.f1;
                }
            }
            // emit a new record with the center id and the data point.
            return new Tuple3<Integer, Vector, Vector>(closestCentroidId, closestCendroid, p);
        }
    }

    /**
     * Appends a count variable to the tuple.
     */
    @FunctionAnnotation.ForwardedFields("f0;f1")
    public static final class CountAppender implements MapFunction<Tuple3<Integer, Vector, Vector>, Tuple3<Integer, Vector, Long>> {
        @Override
        public Tuple3<Integer, Vector, Long> map(Tuple3<Integer, Vector, Vector> t) {
            return new Tuple3<Integer, Vector, Long>(t.f0, t.f2, 1L);
        }
    }

    /**
     * Finds the closest centroid, and calculate the sq. distance to the closest centre as cost
     */
    public static class CostFinder extends RichMapFunction<Vector, Tuple2<Vector, Double>> {

        private Collection<Vector> centroids;

        // Reads the centroid values from a broadcast variable into a collection
        @Override
        public void open(Configuration parameters) throws Exception {
            this.centroids = getRuntimeContext().getBroadcastVariable("centroids");
        }

        @Override
        public Tuple2<Vector, Double> map(Vector p) throws Exception {
            double minDistance = Double.MAX_VALUE;
            SquaredEuclideanDistanceMetric squaredEuclideanDistanceMetric = new SquaredEuclideanDistanceMetric();
            // check all cluster centers
            for (Vector centroid : centroids) {
                // compute distance, using SquaredEuclideanDistanceMetric
                // Note: We need only the squared value for the comparison.
                double distance = squaredEuclideanDistanceMetric.distance(p, centroid);
                // update nearest cluster if necessary
                if (distance < minDistance) {
                    minDistance = distance;
                }
            }
            // emit a new record with the data point and calculated cost
            return new Tuple2(p, minDistance);
        }
    }

    /**
     * Retain only the points which fits into the given probability distribution
     */

    public static class ProbabilitySamplingFilter extends RichFilterFunction<Tuple2<Vector, Double>> {

        private double sumCosts;
        private int overSamplingFactor;
        private Random random;

        public ProbabilitySamplingFilter(int l, long seed) {
            this.overSamplingFactor = l;
            this.random = new Random(seed);
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            Collection<Tuple2<Vector, Double>> aggregatePointCosts = getRuntimeContext().getBroadcastVariable("sumCosts");
            for (Tuple2<Vector, Double> entry : aggregatePointCosts) {
                sumCosts += entry.f1;
            }
        }

        @Override
        public boolean filter(Tuple2<Vector, Double> pointCost) throws Exception {
            return this.random.nextDouble() < (this.overSamplingFactor * pointCost.f1 / sumCosts) ? true : false;
        }
    }

    /**
     * Sums and counts point coordinates.
     */
    public static final class CentroidAccumulator implements ReduceFunction<Tuple3<Integer, Vector, Long>> {
        @Override
        public Tuple3<Integer, Vector, Long> reduce(Tuple3<Integer, Vector, Long> val1, Tuple3<Integer, Vector, Long> val2) {
            return new Tuple3<Integer, Vector, Long>(val1.f0, addVectors(val1.f1, val2.f1), val1.f2 + val2.f2);
        }

        private Vector addVectors(Vector v1, Vector v2) {
            double fields[] = new double[v1.size()];
            for (int i = 0; i < v1.size(); i++) {
                fields[i] = v1.apply(i) + v2.apply(i);
            }
            return new DenseVector(fields);
        }
    }

    /**
     * Computes new centroid from coordinate sum and count of points.
     */
    public static final class CentroidAverager implements MapFunction<Tuple3<Integer, Vector, Long>, Tuple2<Integer, Vector>> {
        @Override
        public Tuple2<Integer, Vector> map(Tuple3<Integer, Vector, Long> value) {
            return new Tuple2<>(value.f0, divideVectorByScalar(value.f1, value.f2));
        }

        private Vector divideVectorByScalar(Vector v, Long s) {
            double fields[] = new double[v.size()];
            for (int i = 0; i < v.size(); i++) {
                fields[i] = v.apply(i) / s;
            }
            return new DenseVector(fields);
        }
    }

    /**
     * Convert the vector data to desired String representation
     */
    public static class KMeansOutputFormat implements MapFunction<Tuple3<Integer, Vector, Vector>, Tuple2<Integer, String>> {
        @Override
        public Tuple2<Integer, String> map(Tuple3<Integer, Vector, Vector> value) throws Exception {
            StringBuilder out = new StringBuilder();
            for (int i = 0; i < value.f2.size(); i++) {
                out.append(Constants.DELIMITER);
                out.append(value.f2.apply(i));
            }
            return new Tuple2(value.f0, out.toString());
        }
    }

    /**
     * Check if the clusters are converged, based on the provided threshold
     */
    public static class ConvergenceEvaluator implements FlatMapFunction<Tuple2<Tuple2<Integer, Vector>, Tuple2<Integer, Vector>>, Tuple2<Integer, Vector>> {

        private double threshold;
        public ConvergenceEvaluator(double threshold) {
            this.threshold = threshold;
        }

        @Override
        public void flatMap(Tuple2<Tuple2<Integer, Vector>, Tuple2<Integer, Vector>> val, Collector<Tuple2<Integer, Vector>> collector) throws Exception {
            if (!evaluateConvergence(val.f0.f1, val.f1.f1, threshold)) {
                collector.collect(val.f0);
            }
        }
        private boolean evaluateConvergence(Vector v1, Vector v2, double threshold) {
            SquaredEuclideanDistanceMetric distanceMetric = new SquaredEuclideanDistanceMetric();
            return (distanceMetric.distance(v1, v2) <= threshold * threshold);
        }
    }

    /**
     * Implementation of local kMeans using kMeans++ initialization
     * Adapted from Apache Spark Implementation: LocalKMeans.scala
     */

    public static class LocalKMeans implements GroupReduceFunction<Tuple3<Integer, Vector, Long>, Vector> {
        private int k;
        private int maxIter;
        private Random random;
        private int dimensions;

        public LocalKMeans(int k, int maxIter) {
            this.k = k;
            this.maxIter = maxIter;
            this.random = new Random(Long.MAX_VALUE);
        }

        @Override
        public void reduce(Iterable<Tuple3<Integer, Vector, Long>> iterable, Collector<Vector> collector) throws Exception {

            // Identify initial k centres using kmeans ++

            List<Vector> points = new ArrayList<Vector>();
            List<Long> weights = new ArrayList<Long>();
            List<Double> costs = new ArrayList<Double>();
            Vector centres[] = new Vector[k];

            SquaredEuclideanDistanceMetric squaredEuclideanDistanceMetric = new SquaredEuclideanDistanceMetric();

            for (Tuple3 val : iterable) {
                points.add((Vector) val.f1);
                weights.add((Long) val.f2);
            }

            dimensions = points.get(0).size();
            System.out.println("Dimensions = " + dimensions);

            Vector initialCentre = getInitialCentre(points, weights);
            centres[0] = initialCentre;

            for (int i = 0; i < points.size(); i++) {
                costs.add(i, squaredEuclideanDistanceMetric.distance(points.get(i), centres[0]));

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
                    costs.add(index, Math.min(squaredEuclideanDistanceMetric.distance(points.get(i), centres[i]), costs.get(index)));

                }
            }

            // Perform Lloyd's algorithm iterations

            for (int iteration = 0; iteration < maxIter; iteration++) {
                Map<Vector, List<Vector>> clusterMembers = new HashMap<Vector, List<Vector>>();
                for (int i = 0; i < centres.length; i++) {
                    clusterMembers.put(centres[i], new ArrayList<Vector>());
                }
                for (int i = 0; i < points.size(); i++) {
                    double minDistance = Double.MAX_VALUE;
                    Vector correctCentre = null;
                    for (int j = 0; j < centres.length; j++) {
                        double distance = squaredEuclideanDistanceMetric.distance(points.get(i), centres[j]);
                        if (distance < minDistance) {
                            minDistance = distance;
                            correctCentre = centres[j];
                        }
                    }
                    clusterMembers.get(correctCentre).add(points.get(i));
                }
                for (int i = 0; i < centres.length; i++) {
                    centres[i] = findCentroidOfVectors(clusterMembers.get(centres[i]), dimensions);

                }
            }

            // The updated centres
            for (Vector centre : centres) {
                collector.collect(centre);
            }
        }

        /**
         * Find the centroid of given vectors
         *
         * @param vectors
         * @param dimensions
         * @return centroidVector
         */
        private Vector findCentroidOfVectors(List<Vector> vectors, int dimensions) {

            int size = vectors.size();
            double fields[] = new double[dimensions];
            Vector cnetroidVector = new DenseVector(fields);

            for (Vector v : vectors) {
                for (int i = 0; i < v.size(); i++) {
                    cnetroidVector.update(i, cnetroidVector.apply(i) + v.apply(i));
                }
            }

            for (int i = 0; i < cnetroidVector.size(); i++) {
                cnetroidVector.update(i, cnetroidVector.apply(i) / size);
            }
            return cnetroidVector;
        }

        /**
         * Get the initial centre from the weighted points
         *
         * @param points
         * @param weights
         * @return initialCentre
         */
        private Vector getInitialCentre(List<Vector> points, List<Long> weights) {
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
