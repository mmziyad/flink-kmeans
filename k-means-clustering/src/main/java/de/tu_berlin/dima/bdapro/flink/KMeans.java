package de.tu_berlin.dima.bdapro.flink;

import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.DataSetUtils;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.ml.math.DenseVector;
import org.apache.flink.ml.math.Vector;
import org.apache.flink.ml.metrics.distances.SquaredEuclideanDistanceMetric;
import org.apache.flink.util.Collector;

import java.util.Collection;

/**
 * Created by zis on 04/12/16.
 * Sample parameters: --input k-means-clustering/src/main/resources/kmeans_input.txt --k 2 --iterations 20
 */
public class Kmeans {
    public static void main(String[] args) throws Exception {

        // Checking input parameters
        final ParameterTool params = ParameterTool.fromArgs(args);
        if (!params.has("input")) throw new Exception();

        // set up execution environment
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(params);

        // get input data:
        DataSet<Vector> points = env
                .readTextFile(params.get("input"))
                .map(new VectorizedData());

        // get the number of clusters
        int k = params.getInt("k", 2);
        // get the number of iterations;
        int maxIter = params.getInt("iterations", 10);
        // get the threshold for convergence
        double threshold = params.getDouble("threshold" ,0.0);

        // derive initial cluster centres randomly from input vectors
        DataSet<Tuple2<Integer, Vector>> centroids = DataSetUtils
                .sampleWithSize(points, false, k, Long.MAX_VALUE)
                .reduceGroup(new CentroidLabeler());

        // Use Bulk iteration specifying max possible itrations
        // If the clusters converge before that, the iteration will stop.
        IterativeDataSet<Tuple2<Integer, Vector>> loop = centroids.iterate(maxIter);

        // Execution of the Kmeans algorithm
        DataSet<Tuple2<Integer, Vector>> newCentroids = points
                // compute closest centroid for each point
                .map(new SelectNearestCenter()).withBroadcastSet(loop, "centroids")
                // count and sum point coordinates for each centroid
                .map(new CountAppender())
                // group by the centroid ID
                .groupBy(0)
                .reduce(new CentroidAccumulator())
                // compute new centroids from point counts and coordinate sums
                .map(new CentroidAverager());

        // Join the new centroid dataset with the previous centroids
        DataSet<Tuple2<Tuple2<Integer, Vector>, Tuple2<Integer, Vector>>> compareSet = newCentroids
                .join(loop)
                .where(0)
                .equalTo(0);

        //Evaluate whether the cluster centres are converged (if so, return empy data set)
        DataSet<Tuple2<Integer, Vector>> terminationSet = compareSet
                .flatMap(new ConvergenceEvaluator(threshold));

        // feed new centroids back into next iteration
        // If all the clusters are converged, iteration will stop
        DataSet<Tuple2<Integer, Vector>> finalCentroids = loop.closeWith(newCentroids, terminationSet);

        // assign points to final clusters
        DataSet<String> result = points
                .map(new SelectNearestCenter()).withBroadcastSet(finalCentroids, "centroids")
                .map(new KMeansOutputFormat());

        // emit result
        if (params.has("output")) {
            result.writeAsCsv(params.get("output"), "\n", " ");
            // since file sinks are lazy, we trigger the execution explicitly
            env.execute("KMeans Clustering");
        } else {
            System.out.println("Printing result to stdout. Use --output to specify output path.");
            result.print();
        }
    }

    /**
     * Determines the closest cluster center for a data point.
     */
    @FunctionAnnotation.ForwardedFields("*->1")
    public static final class SelectNearestCenter extends RichMapFunction<Vector, Tuple2<Integer, Vector>> {
        private Collection<Tuple2<Integer, Vector>> centroids;

        // Reads the centroid values from a broadcast variable into a collection
        @Override
        public void open(Configuration parameters) throws Exception {
            this.centroids = getRuntimeContext().getBroadcastVariable("centroids");
        }

        @Override
        public Tuple2<Integer, Vector> map(Vector p) throws Exception {
            double minDistance = Double.MAX_VALUE;
            SquaredEuclideanDistanceMetric squaredEuclideanDistanceMetric = new SquaredEuclideanDistanceMetric();
            int closestCentroidId = -1;
            // check all cluster centers
            for (Tuple2<Integer, Vector> centroid : centroids) {
                // compute distance, using SquaredEuclideanDistanceMetric
                // Note: We need only the squared value for the comparison.
                double distance = squaredEuclideanDistanceMetric.distance(p, centroid.f1);
                // update nearest cluster if necessary
                if (distance < minDistance) {
                    minDistance = distance;
                    closestCentroidId = centroid.f0;
                }
            }
            // emit a new record with the center id and the data point.
            return new Tuple2<Integer, Vector>(closestCentroidId, p);
        }
    }

    /**
     * Appends a count variable to the tuple.
     */
    @FunctionAnnotation.ForwardedFields("f0;f1")
    public static final class CountAppender implements MapFunction<Tuple2<Integer, Vector>, Tuple3<Integer, Vector, Long>> {
        @Override
        public Tuple3<Integer, Vector, Long> map(Tuple2<Integer, Vector> t) {
            return new Tuple3<Integer, Vector, Long>(t.f0, t.f1, 1L);
        }
    }

    /**
     * Sums and counts point coordinates.
     */
    @FunctionAnnotation.ForwardedFields("0")
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

    private static class CentroidLabeler implements GroupReduceFunction<Vector, Tuple2<Integer, Vector>> {
        @Override
        public void reduce(Iterable<Vector> iterable, Collector<Tuple2<Integer, Vector>> collector) throws Exception {
            int label = 0;
            for (Vector v : iterable) {
                collector.collect(new Tuple2<>(label++, v));
            }
        }
    }

    private static class VectorizedData implements MapFunction<String, Vector> {
        @Override
        public Vector map(String value) {
            String fileds[] = value.split(" ");
            double[] fieldVals = new double[fileds.length];
            for (int i = 0; i < fileds.length; i++) {
                fieldVals[i] = Double.parseDouble(fileds[i]);
            }
            Vector point = new DenseVector(fieldVals);
            return point;
        }
    }

    private static class KMeansOutputFormat implements MapFunction<Tuple2<Integer, Vector>, String> {
        @Override
        public String map(Tuple2<Integer, Vector> value) throws Exception {
            StringBuilder out = new StringBuilder();
            out.append(value.f0);
            for (int i = 0; i < value.f1.size(); i++) {
                out.append(" ");
                out.append(value.f1.apply(i));
            }
            return out.toString();
        }
    }

    private static class ConvergenceEvaluator implements FlatMapFunction<Tuple2<Tuple2<Integer, Vector>, Tuple2<Integer, Vector>>, Tuple2<Integer, Vector>> {

        private double threshold;

        public ConvergenceEvaluator(double threshold) {
            this.threshold = threshold;
        }

        @Override
        public void flatMap(Tuple2<Tuple2<Integer, Vector>, Tuple2<Integer, Vector>> val, Collector<Tuple2<Integer, Vector>> collector) throws Exception {
            if (!evaluateConvergence(val.f0.f1, val.f1.f1, threshold)){
                collector.collect(val.f0);
            }
        }
        private boolean evaluateConvergence(Vector v1, Vector v2, double threshold) {
            SquaredEuclideanDistanceMetric  distanceMetric = new SquaredEuclideanDistanceMetric();
            return (distanceMetric.distance(v1, v2) <= threshold*threshold);
        }
    }
}
