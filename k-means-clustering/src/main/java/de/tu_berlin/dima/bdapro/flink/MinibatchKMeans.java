package de.tu_berlin.dima.bdapro.flink;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.io.TextOutputFormat;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.DataSetUtils;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.ml.math.DenseVector;
import org.apache.flink.ml.math.Vector;
import org.apache.flink.ml.metrics.distances.SquaredEuclideanDistanceMetric;

import java.util.Collection;

/**
 * Created by JML on 12/9/16.
 */
public class MinibatchKMeans {

    public static void main(String[] args) throws Exception {

        //1. Get input
        final ParameterTool params = ParameterTool.fromArgs(args);
        String inputFile = params.get("input");
        String outputDir = params.get("output");
        String batchSizeStr = params.get("batchSize");
        String kStr = params.get("k");
        String iterationStr = params.get("iterations");

        //check required input
        if (inputFile == null || batchSizeStr == null || kStr == null || iterationStr == null) {
            throw new Exception();
        }

        int batchSize = Integer.parseInt(batchSizeStr);
        int k = Integer.parseInt(kStr);
        int iterations = Integer.parseInt(iterationStr);

        // set up execution environment
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(params);

        process(env, inputFile, outputDir, batchSize, iterations, k);

    }

    public static void process(ExecutionEnvironment env, String inputFile, String outputDir, int batchSize, int nbOfIterations, int nbOfClusters) throws Exception {
        // 1.Select sample
        DataSet<Vector> dataPoints = env.readTextFile(inputFile).map(new MapFunction<String, Vector>() {
            public Vector map(String value) {
                String fields[] = value.split(" ");
                double[] fieldVals = new double[fields.length];
                for (int i = 0; i < fields.length; i++) {
                    fieldVals[i] = Double.parseDouble(fields[i]);
                }
                Vector point = new DenseVector(fieldVals);
                return point;
            }
        });

        // Randomly choose initial centroids
        IterativeDataSet<Vector> loopCentroids = DataSetUtils.sampleWithSize(dataPoints, false, nbOfClusters, Long.MAX_VALUE).iterate(nbOfIterations);

        DataSet<Vector> miniBatch = DataSetUtils.sampleWithSize(dataPoints, false, batchSize, Long.MAX_VALUE);

        // 2.Map each data points to centers
        DataSet<Vector> centroids = miniBatch.map(new SelectNearestCenter()).withBroadcastSet(loopCentroids, "centroids")
                // 3.Group data points according to center
                .groupBy(0)
                // 4.Use reduce to calculate centroids by gradient learning rate
                .reduce(new CalculateCentroidByGradient())
                .map(new GetCentroidVector());

        // 5.Close loop
        DataSet<Vector> finalCentroids = loopCentroids.closeWith(centroids);

        DataSet<Tuple3<Integer, Vector, Integer>> clusteredPoints = dataPoints
                // assign points to final clusters
                .map(new SelectNearestCenter()).withBroadcastSet(finalCentroids, "centroids");

        DataSet<String> result = clusteredPoints.map(new MapFunction<Tuple3<Integer, Vector, Integer>, String>() {
            @Override
            public String map(Tuple3<Integer, Vector, Integer> value) throws Exception {
                StringBuilder out = new StringBuilder();
                out.append(value.f0 + " ");
                out.append(Utils.vectorToCustomString(value.f1));
                return out.toString();
            }
        });

        // emit result
        if (outputDir != null) {
            clusteredPoints.writeAsFormattedText(outputDir, new TextOutputFormat.TextFormatter<Tuple3<Integer, Vector, Integer>>() {
                @Override
                public String format(Tuple3<Integer, Vector, Integer> value) {
                    return value.f0 + " " + Utils.vectorToCustomString(value.f1);
                }
            });
            // since file sinks are lazy, we trigger the execution explicitly
            env.execute("Minibatch K-means Clustering");
            // TODO: this is just for testing, remove when benchmarking
            Utils.mergeFile(outputDir, outputDir + "/merged");
        } else {
            System.out.println("Printing result to stdout. Use --output to specify output path.");
            result.print();
        }
    }


    /**
     * Determines the closest cluster center for a data point.
     */
    @FunctionAnnotation.ForwardedFields("*->1")
    public static final class SelectNearestCenter extends RichMapFunction<Vector, Tuple3<Integer, Vector, Integer>> {
        private Collection<Vector> centroids;

        /**
         * Reads the centroid values from a broadcast variable into a collection.
         */
        @Override
        public void open(Configuration parameters) throws Exception {
            this.centroids = getRuntimeContext().getBroadcastVariable("centroids");
        }

        @Override
        public Tuple3<Integer, Vector, Integer> map(Vector p) throws Exception {

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
            // emit a new record with the center id, the data point, count =1
            return new Tuple3<Integer, Vector, Integer>(closestCentroidId, p, 1);
        }
    }


    public static final class CalculateCentroidByGradient implements ReduceFunction<Tuple3<Integer, Vector, Integer>> {

        public Tuple3<Integer, Vector, Integer> reduce(Tuple3<Integer, Vector, Integer> center, Tuple3<Integer, Vector, Integer> point) throws Exception {
            // point.f2 = 1
            center.f2 = center.f2 + point.f2;
            // calculate learning rate by count of data points which belongs to cluster
            long learningRate = 1 / center.f2;
            Vector newCenter = calculateCenter(center.f1, point.f1, learningRate);
            return new Tuple3<Integer, Vector, Integer>(center.f0, newCenter, center.f2);
        }

        private Vector calculateCenter(Vector v1, Vector v2, long learningRate) {
            double fields[] = new double[v1.size()];
            for (int i = 0; i < v1.size(); i++) {
                fields[i] = (1 - learningRate) * v1.apply(i) + learningRate * v2.apply(i);
            }

            return new DenseVector(fields);
        }
    }

    public static final class GetCentroidVector implements MapFunction<Tuple3<Integer, Vector, Integer>, Vector> {

        public Vector map(Tuple3<Integer, Vector, Integer> center) throws Exception {
            return center.f1;
        }
    }
}