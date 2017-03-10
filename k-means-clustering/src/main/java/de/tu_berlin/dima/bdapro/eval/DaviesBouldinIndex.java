package de.tu_berlin.dima.bdapro.eval;

import de.tu_berlin.dima.bdapro.datatype.Centroid;
import de.tu_berlin.dima.bdapro.datatype.Point;
import de.tu_berlin.dima.bdapro.util.UDFs;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichGroupReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.util.Collector;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by zis on 07/01/17.
 * Ref:
 * [1] https://en.wikipedia.org/wiki/Davies–Bouldin_index
 * [2] https://github.com/gm-spacagna/tunup/blob/master/src/org/tunup/modules/kmeans/evaluation/DaviesBouldinScore.java
 */
public class DaviesBouldinIndex {

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

        // Find intraCluster Distance for each cluster
        // Then, find the average intracluster distance
        DataSet<Tuple2<Integer, Double>> intraClusterDistance = clusterMembers
                .map(new UDFs.intraClusterDistance(1)).withBroadcastSet(clusterCentres, "centroids")
                .groupBy(0)
                .reduce(new ReduceFunction<Tuple4<Integer, Point, Long, Double>>() {
                    @Override
                    public Tuple4<Integer, Point, Long, Double> reduce(Tuple4<Integer, Point, Long, Double> value1, Tuple4<Integer, Point, Long, Double> value2) throws Exception {
                        return new Tuple4<>(value1.f0, value1.f1, value1.f2 + value2.f2, value1.f3 + value2.f3);
                    }
                }).map(new MapFunction<Tuple4<Integer, Point, Long, Double>, Tuple2<Integer, Double>>() {
                    @Override
                    public Tuple2<Integer, Double> map(Tuple4<Integer, Point, Long, Double> value) throws Exception {
                        return new Tuple2<>(value.f0, value.f3 / value.f2);
                    }
                });

        // Calculate Davies Bouldin Index
        DataSet<Double> daviesBouldinIndex = intraClusterDistance
                .reduceGroup(new RichGroupReduceFunction<Tuple2<Integer, Double>, Double>() {

                    Map<Integer, Centroid> clusterCentres;
                    Map<Integer, Double> intraClusterDistances;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        clusterCentres = new HashMap<>();
                        intraClusterDistances = new HashMap<>();
                        Collection<Centroid> centroids = getRuntimeContext().getBroadcastVariable("centroids");
                        for (Centroid c : centroids) {
                            clusterCentres.put(c.getId(), c);
                        }
                    }

                    @Override
                    public void reduce(Iterable<Tuple2<Integer, Double>> values, Collector<Double> out) throws Exception {
                        for (Tuple2<Integer, Double> val : values) {
                            intraClusterDistances.put(val.f0, val.f1);
                        }

                        double result = 0.0;
                        for (int i = 0; i < clusterCentres.size(); i++) {
                            double max = Double.NEGATIVE_INFINITY;
                            for (int j = 0; j < clusterCentres.size(); j++) {
                                if (i != j) {
                                    double val = (intraClusterDistances.get(i) + intraClusterDistances.get(j))
                                            / clusterCentres.get(i).euclideanDistance(clusterCentres.get(j));
                                    if (val > max)
                                        max = val;
                                }
                            }
                            result = result + max;
                        }
                        out.collect(result / clusterCentres.size());
                    }
                }).withBroadcastSet(clusterCentres, "centroids");

        // Prepare output
        if (params.has("output")) {
            daviesBouldinIndex.writeAsText(params.get("output"), FileSystem.WriteMode.OVERWRITE);
            // since file sinks are lazy, we trigger the execution explicitly
            env.execute("Davies Bouldin Index");
        } else {
            System.out.println("Printing result to stdout. Use --output to specify output path.");
            daviesBouldinIndex.print();
        }
    }
}
