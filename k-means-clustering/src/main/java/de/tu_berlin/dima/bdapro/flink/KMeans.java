package de.tu_berlin.dima.bdapro.flink;

import de.tu_berlin.dima.bdapro.util.Constants;
import de.tu_berlin.dima.bdapro.util.UDFs;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.DataSetUtils;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.ml.math.Vector;

/**
 * Created by zis on 04/12/16.
 * Usage: --input k-means-clustering/src/main/resources/kmeans_input.txt --output /tmp/test --k 2 --iterations 20
 */
public class KMeans {
    public static void main(String[] args) throws Exception {

        // Checking input parameters
        final ParameterTool params = ParameterTool.fromArgs(args);
        if (!params.has("input")) throw new Exception("Input Data is not specified");

        // set up execution environment
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(params);

        // get input data:
        DataSet<Vector> points = env.readTextFile(params.get("input"))
                .map(new UDFs.VectorizedData());

        // get the number of clusters
        int k = params.getInt("k", 2);

        // get the number of iterations;
        int maxIter = params.getInt("iterations", 10);

        // get the threshold for convergence
        double threshold = params.getDouble("threshold" ,0.0);

        // derive initial cluster centres randomly from input vectors
        DataSet<Tuple2<Integer, Vector>> centroids = DataSetUtils
                .sampleWithSize(points, false, k, Long.MAX_VALUE)
                .reduceGroup(new UDFs.CentroidLabeler());

        // Use Bulk iteration specifying max possible iterations
        // If the clusters converge before that, the iteration will stop.
        IterativeDataSet<Tuple2<Integer, Vector>> loop = centroids.iterate(maxIter);

        // Execution of the kMeans algorithm
        DataSet<Tuple2<Integer, Vector>> newCentroids = points
                // compute closest centroid for each point
                .map(new UDFs.SelectNearestCenter()).withBroadcastSet(loop, "centroids")
                // count and sum point coordinates for each centroid
                .map(new UDFs.CountAppender())
                // group by the centroid ID
                .groupBy(0)
                .reduce(new UDFs.CentroidAccumulator())
                // compute new centroids from point counts and coordinate sums
                .map(new UDFs.CentroidAverager());

        // Join the new centroid dataset with the previous centroids
        DataSet<Tuple2<Tuple2<Integer, Vector>, Tuple2<Integer, Vector>>> compareSet = newCentroids
                .join(loop)
                .where(0)
                .equalTo(0);

        //Evaluate whether the cluster centres are converged (if so, return empy data set)
        DataSet<Tuple2<Integer, Vector>> terminationSet = compareSet
                .flatMap(new UDFs.ConvergenceEvaluator(threshold));

        // feed new centroids back into next iteration
        // If all the clusters are converged, iteration will stop
        DataSet<Tuple2<Integer, Vector>> finalCentroids = loop.closeWith(newCentroids, terminationSet);

        // assign points to final clusters
        DataSet<Tuple2<Integer, String>> result = points
                .map(new UDFs.SelectNearestCenter()).withBroadcastSet(finalCentroids, "centroids")
                .map(new UDFs.KMeansOutputFormat());

        // emit result
        if (params.has("output")) {
            //finalCentroids.writeAsCsv(params.get("output"), "\n", Constants.DELIMITER, FileSystem.WriteMode.OVERWRITE);
            result.writeAsCsv(params.get("output"), "\n", Constants.DELIMITER, FileSystem.WriteMode.OVERWRITE);
            // since file sinks are lazy, we trigger the execution explicitly
            env.execute("kMeans Clustering");
        } else {
            System.out.println("Printing result to stdout. Use --output to specify output path.");
            //finalCentroids.print();
            result.print();
        }
    }
}
