package de.tu_berlin.dima.bdapro.flink;

import de.tu_berlin.dima.bdapro.datatype.Point;
import de.tu_berlin.dima.bdapro.util.Constants;
import de.tu_berlin.dima.bdapro.util.UDFs;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.DataSetUtils;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;

/**
 * Created by zis on 30/12/16.
 */
public class InitKMeansParallel {

    public static void main(String[] args) throws Exception {

        // Checking input parameters
        final ParameterTool params = ParameterTool.fromArgs(args);
        if (!params.has("input")) throw new Exception("Input Data is not specified");

        if (!params.has("d"))
            throw new Exception("No of Dimensions is not specified");


        // set up execution environment
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(params);

        // get the number of dimensions
        int d = params.getInt("d");

        // get the number of clusters
        int k = params.getInt("k", 2);

        // get value of oversampling factor
        int l = params.getInt("l", 2 * k);

        // get the number of iterations;
        int maxIter = params.getInt("iterations", 10);

        // get the number of initialization steps to perform
        int initializationSteps = params.getInt("initializationSteps", 2);

        // get the threshold for convergence
        double threshold = params.getDouble("threshold", 0);

        // get input data:
        DataSet<Point> points = env
                .readTextFile(params.get("input"))
                .map(new UDFs.PointData(d));

        // derive initial cluster centre randomly from input Points, and add to centres dataset
        DataSet<Point> centres = DataSetUtils
                .sampleWithSize(points, false, 1, Long.MAX_VALUE);

        // Perform Iterations for the given initializationSteps. Usually 2 is enough
        IterativeDataSet<Point> chosen = centres.iterate(initializationSteps);

        // Calculate the cost for each data point
        DataSet<Tuple2<Point, Double>> pointCosts = points
                .map(new UDFs.CostFinder()).withBroadcastSet(chosen, "centroids");

        // Calculate the sum of costs
        DataSet<Tuple2<Point, Double>> sumCosts = pointCosts.sum(1);

        DataSet<Point> newCentres = pointCosts
                .filter(new UDFs.ProbabilitySamplingFilter(l, Long.MAX_VALUE)).withBroadcastSet(sumCosts, "sumCosts")
                .map(new MapFunction<Tuple2<Point, Double>, Point>() {
                    @Override
                    public Point map(Tuple2<Point, Double> pointCost) throws Exception {
                        return pointCost.f0;
                    }
                });
        // Update the centres
        centres = chosen.union(newCentres);

        // Close the iterative step with updated centres
        DataSet<Point> finalCentres = chosen.closeWith(centres);

        // Label the centroids from 0 to n
        DataSet<Tuple2<Integer, Point>> labeledCentres = finalCentres
                .reduceGroup(new UDFs.CentroidLabeler());

        // Calculate the weights for each centre, as the number of points
        // for which the centre is identified as the closest centre.

        DataSet<Tuple3<Integer, Point, Long>> weightedPoints = points
                .map(new UDFs.SelectNearestCenter()).withBroadcastSet(labeledCentres, "centroids")
                .map(new UDFs.CountAppender())
                .groupBy(0)
                .sum(2);

        // Apply kMeans++ to select k centres from the kMeans|| result set.
        // Since the number of points will be ~ (2*k), a single reduceGroup is enough to perform kMeans++

        DataSet<Tuple2<Integer, Point>> centroids = weightedPoints
                .reduceGroup(new UDFs.LocalKMeans(d, k, maxIter))
                .reduceGroup(new UDFs.CentroidLabeler());


        // Perform Lloyd's algorithm on the entire data set

        // Use Bulk iteration specifying max possible iterations
        // If the clusters converge before that, the iteration will stop.
        IterativeDataSet<Tuple2<Integer, Point>> loop = centroids.iterate(maxIter);

        // Execution of the kMeans algorithm
        DataSet<Tuple2<Integer, Point>> newCentroids = points
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
        DataSet<Tuple2<Tuple2<Integer, Point>, Tuple2<Integer, Point>>> compareSet = newCentroids
                .join(loop)
                .where(0)
                .equalTo(0);

        //Evaluate whether the cluster centres are converged (if so, return empy data set)
        DataSet<Tuple2<Integer, Point>> terminationSet = compareSet
                .flatMap(new UDFs.ConvergenceEvaluator(threshold));

        // feed new centroids back into next iteration
        // If all the clusters are converged, iteration will stop
        DataSet<Tuple2<Integer, Point>> finalCentroids = loop.closeWith(newCentroids, terminationSet);

        // assign points to final clusters
        DataSet<Tuple2<Integer, Point>> result = points
                .map(new UDFs.SelectNearestCenter()).withBroadcastSet(finalCentroids, "centroids");

        // emit result
        if (params.has("output")) {
            //finalCentroids.writeAsCsv(params.get("output"), "\n", Constants.DELIMITER, FileSystem.WriteMode.OVERWRITE);
            result.writeAsCsv(params.get("output"), "\n", Constants.DELIMITER, FileSystem.WriteMode.OVERWRITE);
            env.execute("kMeans|| Clustering");
        } else {
            System.out.println("Printing result to stdout. Use --output to specify output path.");
            //finalCentroids.print();
            result.print();
        }
    }
}
