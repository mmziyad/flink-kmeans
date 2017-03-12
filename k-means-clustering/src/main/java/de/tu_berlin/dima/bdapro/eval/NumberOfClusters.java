package de.tu_berlin.dima.bdapro.eval;

import de.tu_berlin.dima.bdapro.datatype.Centroid;
import de.tu_berlin.dima.bdapro.datatype.Point;
import de.tu_berlin.dima.bdapro.util.UDFs;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.util.Collector;

/**
 * Created by zis on 11/03/17.
 */
public class NumberOfClusters {

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

        //Count the number of clusters
        DataSet<Integer> numOfClusters = clusterCentres.reduceGroup(new GroupReduceFunction<Centroid, Integer>() {
            @Override
            public void reduce(Iterable<Centroid> iterable, Collector<Integer> collector) throws Exception {

                for(Centroid c: iterable){
                    collector.collect(c.getId());
                }
            }
        });

        // result
        numOfClusters.print();
    }
}
