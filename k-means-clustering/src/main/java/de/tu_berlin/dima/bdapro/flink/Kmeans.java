package de.tu_berlin.dima.bdapro.flink;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
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

import java.util.Collection;

/**
 * Created by zis on 04/12/16.
 */
public class Kmeans {

    public static void main(String[] args) throws Exception {

        // Checking input parameters
        final ParameterTool params = ParameterTool.fromArgs(args);

        // set up execution environment
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(params);

        if (!params.has("input")) throw new Exception();

        // get input data:
        DataSet<Vector> points = env.readTextFile(params.get("input")).map(new MapFunction<String, Vector>() {
            public Vector map(String value) {
                String fileds[] = value.split(" ");
                double[] fieldVals = new double[fileds.length];
                for (int i = 0; i < fileds.length; i++) {
                    fieldVals[i] = Double.parseDouble(fileds[i]);
                }
                Vector point = new DenseVector(fieldVals);
                return point;
            }
        });

        // get the number of clusters
        int k = Integer.parseInt(params.get("k"));

        // derive initial cluster centres randomly from input vectors
        DataSet<Vector> centroids = DataSetUtils.sampleWithSize(points, false, k, Long.MAX_VALUE);

        // set number of bulk iterations for KMeans algorithm
        IterativeDataSet<Vector> loop = centroids.iterate(params.getInt("iterations", 10));


        DataSet<Vector> newCentroids = points
                // compute closest centroid for each point
                .map(new SelectNearestCenter()).withBroadcastSet(loop, "centroids")
                // count and sum point coordinates for each centroid
                .map(new CountAppender())
                .groupBy(0).reduce(new CentroidAccumulator())
                // compute new centroids from point counts and coordinate sums
                .map(new CentroidAverager());

        // feed new centroids back into next iteration
        DataSet<Vector> finalCentroids = loop.closeWith(newCentroids);

        DataSet<Tuple2<Integer, Vector>> clusteredPoints = points
                // assign points to final clusters
                .map(new SelectNearestCenter()).withBroadcastSet(finalCentroids, "centroids");

        DataSet<String> result = clusteredPoints.map(new MapFunction<Tuple2<Integer, Vector>, String>() {
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
        });

        // emit result
        if (params.has("output")) {
            clusteredPoints.writeAsCsv(params.get("output"), "\n", " ");

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

        private Collection<Vector> centroids;

        /**
         * Reads the centroid values from a broadcast variable into a collection.
         */
        @Override
        public void open(Configuration parameters) throws Exception {
            this.centroids = getRuntimeContext().getBroadcastVariable("centroids");
        }

        @Override
        public Tuple2<Integer, Vector> map(Vector p) throws Exception {

            double minDistance = Double.MAX_VALUE;
            SquaredEuclideanDistanceMetric squaredEuclideanDistanceMetric = new SquaredEuclideanDistanceMetric();
            int closestCentroidId = -1;
            int position = 0;

            // check all cluster centers
            for (Vector centroid : centroids) {

                // increment the position variable to identify cluster ID
                position++;
                // compute distance, using SquaredEuclideanDistanceMetric
                // We need only the squared value for the comparison.
                double distance = squaredEuclideanDistanceMetric.distance(p, centroid);

                // update nearest cluster if necessary
                if (distance < minDistance) {
                    minDistance = distance;
                    closestCentroidId = position;
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
    public static final class CentroidAverager implements MapFunction<Tuple3<Integer, Vector, Long>, Vector> {

        @Override
        public Vector map(Tuple3<Integer, Vector, Long> value) {

            return divideVectorByScalar(value.f1, value.f2);
        }

        private Vector divideVectorByScalar(Vector v, Long s) {

            double fields[] = new double[v.size()];

            for (int i = 0; i < v.size(); i++) {
                fields[i] = v.apply(i) / s;
            }
            return new DenseVector(fields);
        }
    }
}
