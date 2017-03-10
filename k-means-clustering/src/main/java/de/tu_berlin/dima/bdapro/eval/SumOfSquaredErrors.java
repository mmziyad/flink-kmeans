package de.tu_berlin.dima.bdapro.eval;

import de.tu_berlin.dima.bdapro.datatype.Centroid;
import de.tu_berlin.dima.bdapro.datatype.Point;
import de.tu_berlin.dima.bdapro.util.UDFs;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;

/**
 * Created by zis on 07/01/17.
 * Calculate the sum of squared errors (SSE) for the cluster distribution
 */
public class SumOfSquaredErrors {

    public static void main(String[] args) throws Exception {

        // Checking input parameters
        final ParameterTool params = ParameterTool.fromArgs(args);
        if (!params.has("input"))
            throw new Exception("Input Data is not specified");
        if (!params.has("d"))
            throw new Exception("No of Dimensions is not specified");

        // get the number of dimensions
        int d = params.getInt("d");

        // set up execution environment
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // make parameters available in the web intraface
        env.getConfig().setGlobalJobParameters(params);

        // Read the output of KMeans clustering, and append 1L to each record
        DataSet<Tuple3<Integer, Point, Long>> clusterMembers = env.readTextFile(params.get("input"))
                .map(new UDFs.ClusterMembership(d))
                .map(new UDFs.CountAppender());

        // Find the cluster centres
        DataSet<Centroid> clusterCentres = clusterMembers
                .groupBy(0)
                .reduce(new UDFs.CentroidAccumulator())
                .map(new UDFs.CentroidAverager());

        // calculate the SSE
        DataSet<Double> sumOfSquaredErrors = clusterMembers
                .map(new UDFs.intraClusterDistance(2)).withBroadcastSet(clusterCentres, "centroids")
                .reduce(new ReduceFunction<Tuple4<Integer, Point, Long, Double>>() {
                    @Override
                    public Tuple4<Integer, Point, Long, Double> reduce(Tuple4<Integer, Point, Long, Double> val1, Tuple4<Integer, Point, Long, Double> val2) throws Exception {
                        return new Tuple4<>(val1.f0, val1.f1, val1.f2, val1.f3 + val2.f3);
                    }
                }).map(new MapFunction<Tuple4<Integer, Point, Long, Double>, Double>() {
                    @Override
                    public Double map(Tuple4<Integer, Point, Long, Double> val) throws Exception {
                        return val.f3;
                    }
                });

        // Prepare output
        if (params.has("output")) {
            sumOfSquaredErrors.writeAsText(params.get("output"), FileSystem.WriteMode.OVERWRITE);
            // since file sinks are lazy, we trigger the execution explicitly
            env.execute("Sum of Squared Errors");
        } else {
            System.out.println("Printing result to stdout. Use --output to specify output path.");
            sumOfSquaredErrors.print();
        }
    }
}
