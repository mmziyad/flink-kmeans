package de.tu_berlin.dima.bdapro.flink;

import de.tu_berlin.dima.bdapro.datatype.ClusterCenter;
import de.tu_berlin.dima.bdapro.datatype.IndexedPoint;
import de.tu_berlin.dima.bdapro.datatype.Point;
import de.tu_berlin.dima.bdapro.util.Constants;
import de.tu_berlin.dima.bdapro.util.UDFs;
import de.tu_berlin.dima.bdapro.util.Utils;
import org.apache.commons.collections.map.HashedMap;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.operators.JoinOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.DataSetUtils;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.util.Collector;

import java.util.*;

/**
 * Created by JML on 1/5/17.
 */
public class BisectingKMeans {
    public static void main(String[] args) throws Exception {
        //1. Get input
        final ParameterTool params = ParameterTool.fromArgs(args);
        String inputFile = params.get("input");
        String outputDir = params.get("output");
        Integer dimension = params.getInt("d", 2);
        Integer k = params.getInt("k", 2);
        Integer iterations = params.getInt("iterations", 10);
        Float threshold = params.getFloat("threshold", 0);

        //check required input
        if (inputFile == null || k == null || iterations == null || outputDir == null) {
            System.out.println("BisectingKMeans <input> <output> <k> <iterations> [<threshold>]");
            System.exit(1);
        }

        // set up execution environment
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(params);

        int loop = 1;
        // initialize folder for temp data and index
        String tmpPath = "/tmp";
        String indexPath = "/index";

        // loop until get k
        DataSet<IndexedPoint> dataPoints = null;
        // index (summary) of data
        DataSet<ClusterCenter> centerSummary = null;

        HashMap<Integer, DataSet<IndexedPoint>> datasetMap = new HashMap<>();

        while (loop < k) {
            // SOLUTION 1
            // chosen cluster to split
            ClusterCenter dividableLeafNode = null;
            if (loop == 1) {
                // for 1st loop, default split is root center
                dividableLeafNode = getRootCenter(1);

                dataPoints = env
                        .readTextFile(params.get("input"))
                        // for 1st iteration, all points are assigned with default index = 1
                        .map(new Utils.DefaultIndexedPointData());
            } else {
                // load index (summary) from index folder
                centerSummary = env.readTextFile(params.get("output") + indexPath)
                        .map(new MapFunction<String, ClusterCenter>() {
                            @Override
                            public ClusterCenter map(String s) throws Exception {
                                String[] cols = s.split(Constants.DELIMITER);
                                ClusterCenter center = new ClusterCenter(Integer.parseInt(cols[0]), null);
                                center.setRow(Long.parseLong(cols[1]));
                                center.setLeafNode(Boolean.parseBoolean(cols[2]));
                                return center;
                            }
                        });

                // pick cluster to split
                dividableLeafNode = getDividableLeafNode(centerSummary);

//                // load data from temp folder
                dataPoints = env.readTextFile(params.get("output") + tmpPath + "/sub" + dividableLeafNode.getIndex())
                        .map(new MapFunction<String, IndexedPoint>() {
                            @Override
                            public IndexedPoint map(String s) throws Exception {
                                String cols[] = s.split(Constants.DELIMITER);
                                double[] fields = new double[cols.length - 1];
                                for (int i = 0; i < fields.length; i++) {
                                    fields[i] = Double.parseDouble(cols[i + 1]);
                                }
                                Point point = new Point(fields);
                                return new IndexedPoint(Integer.parseInt(cols[0]), point);
                            }
                        });
            }

            // calculate the leaf node to split
            // -> maybe send by broadcast set

            // filter data of chosen leaf node
//            DataSet<IndexedPoint> dividableData = dataPoints.filter(new Utils.FilterDividableDataFunc(dividableLeafNode.getIndex()));
            DataSet<IndexedPoint> dividableData = dataPoints;

            // filter out data of other leaf nodes
//            DataSet<IndexedPoint> undividableData = dataPoints.filter(new Utils.FilterUndividableDataFunc(dividableLeafNode.getIndex()));

            // divide data then union with undivided data
            DataSet<IndexedPoint> result = divideData(dividableData, dividableLeafNode, params.get("output") + indexPath, centerSummary, iterations, threshold, env);

//            // write to temp folder
            DataSet<IndexedPoint> set1 = result.filter(new Utils.FilterDividableDataFunc(dividableLeafNode.getIndex() * 2));
            DataSet<IndexedPoint> set2 = result.filter(new Utils.FilterDividableDataFunc(dividableLeafNode.getIndex() * 2 + 1));

            set1.map(new MapFunction<IndexedPoint, Tuple2<Integer, String>>() {
                @Override
                public Tuple2<Integer, String> map(IndexedPoint indexedPoint) throws Exception {
                    StringBuilder pointStr = new StringBuilder("");
                    int lengthMinus = indexedPoint.getFields().length - 1;
                    for (int i = 0; i < lengthMinus; i++) {
                        pointStr = pointStr.append(indexedPoint.getFields()[i]);
                        pointStr = pointStr.append(" ");
                    }
                    pointStr = pointStr.append(indexedPoint.getFields()[lengthMinus]);
                    return new Tuple2<Integer, String>(indexedPoint.getIndex(), pointStr.toString());
                }
            })
                    .writeAsCsv(params.get("output") + tmpPath + "/sub" + (dividableLeafNode.getIndex() * 2), "\n", Constants.DELIMITER, FileSystem.WriteMode.OVERWRITE);

            set2.map(new MapFunction<IndexedPoint, Tuple2<Integer, String>>() {
                @Override
                public Tuple2<Integer, String> map(IndexedPoint indexedPoint) throws Exception {
                    StringBuilder pointStr = new StringBuilder("");
                    int lengthMinus = indexedPoint.getFields().length - 1;
                    for (int i = 0; i < lengthMinus; i++) {
                        pointStr = pointStr.append(indexedPoint.getFields()[i]);
                        pointStr = pointStr.append(" ");
                    }
                    pointStr = pointStr.append(indexedPoint.getFields()[lengthMinus]);
                    return new Tuple2<Integer, String>(indexedPoint.getIndex(), pointStr.toString());
                }
            })
                    .writeAsCsv(params.get("output") + tmpPath + "/sub" + (dividableLeafNode.getIndex() * 2 + 1), "\n", Constants.DELIMITER, FileSystem.WriteMode.OVERWRITE);

            loop++;
            env.execute("Bisecting Kmeans V11 Clustering");
        }

        // rewrite the index file

        List<ClusterCenter> finalCenters = env.readTextFile(params.get("output") + indexPath)
                .map(new MapFunction<String, ClusterCenter>() {
                    @Override
                    public ClusterCenter map(String s) throws Exception {
                        String[] cols = s.split(Constants.DELIMITER);
                        ClusterCenter center = new ClusterCenter(Integer.parseInt(cols[0]), null);
                        center.setRow(Long.parseLong(cols[1]));
                        center.setLeafNode(Boolean.parseBoolean(cols[2]));
                        return center;
                    }
                }).collect();

        DataSet<IndexedPoint> finalPoints = null;
        for (int i = 0; i < finalCenters.size(); i++) {
            ClusterCenter center = finalCenters.get(i);
            if (center.isLeafNode()) {
                DataSet<IndexedPoint> temp = env.readTextFile(params.get("output") + tmpPath + "/sub" + center.getIndex())
                        .map(new MapFunction<String, IndexedPoint>() {
                            @Override
                            public IndexedPoint map(String s) throws Exception {
                                String cols[] = s.split(Constants.DELIMITER);
                                double[] fields = new double[cols.length - 1];
                                for (int i = 0; i < fields.length; i++) {
                                    fields[i] = Double.parseDouble(cols[i + 1]);
                                }
                                Point point = new Point(fields);
                                return new IndexedPoint(Integer.parseInt(cols[0]), point);
                            }
                        });
                if (finalPoints != null) {
                    finalPoints = temp.union(finalPoints);
                } else {
                    finalPoints = temp;
                }
            }
        }

        finalPoints.map(new MapFunction<IndexedPoint, Tuple2<Integer, String>>() {
            @Override
            public Tuple2<Integer, String> map(IndexedPoint indexedPoint) throws Exception {
                StringBuilder pointStr = new StringBuilder("");
                int lengthMinus = indexedPoint.getFields().length - 1;
                for (int i = 0; i < lengthMinus; i++) {
                    pointStr = pointStr.append(indexedPoint.getFields()[i]);
                    pointStr = pointStr.append(" ");
                }
                pointStr = pointStr.append(indexedPoint.getFields()[lengthMinus]);
                return new Tuple2<Integer, String>(indexedPoint.getIndex(), pointStr.toString());
            }
        })
                .writeAsCsv(params.get("output") + "/result", "\n", Constants.DELIMITER, FileSystem.WriteMode.OVERWRITE);

        env.execute();

    }

    public static DataSet<ClusterCenter> summarize(DataSet<IndexedPoint> data) {
        DataSet<ClusterCenter> centers = data
                .map(new MapFunction<IndexedPoint, Tuple2<Integer, Integer>>() {
                    @Override
                    public Tuple2<Integer, Integer> map(IndexedPoint indexedPoint) throws Exception {
                        return new Tuple2<Integer, Integer>(indexedPoint.getIndex(), 1);
                    }
                })
                .groupBy(0)
                .sum(1)
                .map(new MapFunction<Tuple2<Integer, Integer>, ClusterCenter>() {
                    @Override
                    public ClusterCenter map(Tuple2<Integer, Integer> tuple) throws Exception {
                        ClusterCenter center = new ClusterCenter(tuple.f0, null, true);
                        center.setRow(tuple.f1);
                        return center;
                    }
                });
        return centers;
    }

    public static ClusterCenter getDividableLeafNode(DataSet<ClusterCenter> centers) throws Exception {
        ClusterCenter chosenCenter = centers.reduceGroup(new GroupReduceFunction<ClusterCenter, ClusterCenter>() {
            @Override
            public void reduce(Iterable<ClusterCenter> iterable, Collector<ClusterCenter> collector) throws Exception {
                Iterator<ClusterCenter> centerIter = iterable.iterator();
                long maxSize = 0;
                ClusterCenter maxCenter = null;
                while (centerIter.hasNext()) {
                    ClusterCenter element = centerIter.next();
                    if (element.getRow() > maxSize && element.getRow() >= 2 && element.isLeafNode()) {
                        maxSize = element.getRow();
                        maxCenter = element;
                    }
                }

                collector.collect(maxCenter);
            }
        }).collect().get(0);
        return chosenCenter;
    }

    public static DataSet<ClusterCenter> getDividableLeafNodeDataSet(DataSet<ClusterCenter> centers) throws Exception {
        DataSet<ClusterCenter> chosenCenter = centers.reduceGroup(new GroupReduceFunction<ClusterCenter, ClusterCenter>() {
            @Override
            public void reduce(Iterable<ClusterCenter> iterable, Collector<ClusterCenter> collector) throws Exception {
                Iterator<ClusterCenter> centerIter = iterable.iterator();
                long maxSize = 0;
                ClusterCenter maxCenter = null;
                while (centerIter.hasNext()) {
                    ClusterCenter element = centerIter.next();
                    if (element.getRow() > maxSize && element.getRow() >= 2 && element.isLeafNode()) {
                        maxSize = element.getRow();
                        maxCenter = element;
                    }
                }

                collector.collect(maxCenter);
            }
        });
        return chosenCenter;
    }


    public static DataSet<IndexedPoint> divideData(DataSet<IndexedPoint> data, final ClusterCenter center, String indexDir, DataSet<ClusterCenter> centerSummary, int maxIterations, final float threshold, ExecutionEnvironment env) throws Exception {
        // pick random initial centers to split
        DataSet<ClusterCenter> initialCenters = DataSetUtils.sampleWithSize(data, false, 2, Long.MAX_VALUE)
                .reduceGroup(new GroupReduceFunction<IndexedPoint, ClusterCenter>() {
                    @Override
                    public void reduce(Iterable<IndexedPoint> iterable, Collector<ClusterCenter> collector) throws Exception {
                        Iterator<IndexedPoint> iterator = iterable.iterator();
                        int i = 0;
                        while (iterator.hasNext()) {
                            IndexedPoint pointIdx = iterator.next();
                            ClusterCenter clusterCenter = new ClusterCenter(pointIdx.getIndex() * 2 + i, pointIdx.getFields(), true);
                            collector.collect(clusterCenter);
                            i++;
                        }
                    }
                });

        // iterate maxIterations except convergence condition is met
        IterativeDataSet<ClusterCenter> iterativeCenters = initialCenters.iterate(maxIterations);

        // get closet center
        DataSet<Tuple3<Integer, IndexedPoint, Long>> mappedData = data
                .map(new Utils.SelectNearestCenter())
                .withBroadcastSet(iterativeCenters, "centroids");

        // sum all points and calculate new centers
        DataSet<ClusterCenter> splittedCenters = mappedData
                .groupBy(0)
                .reduce(new Utils.SumClusterPointsReduceFunc())
                .map(new Utils.MapSumToClusterCenterFunc());

        // check convergence with previous centers
        JoinOperator.DefaultJoin<ClusterCenter, ClusterCenter> joinedCenters = splittedCenters
                .join(iterativeCenters)
                .where(new KeySelector<ClusterCenter, Integer>() {
                    @Override
                    public Integer getKey(ClusterCenter clusterCenter) throws Exception {
                        return clusterCenter.getIndex();
                    }
                })
                .equalTo(new KeySelector<ClusterCenter, Integer>() {
                    @Override
                    public Integer getKey(ClusterCenter clusterCenter) throws Exception {
                        return clusterCenter.getIndex();
                    }
                });

        DataSet<Double> convergenceSet = joinedCenters
                .map(new MapFunction<Tuple2<ClusterCenter, ClusterCenter>, Double>() {
                    @Override
                    public Double map(Tuple2<ClusterCenter, ClusterCenter> tuple) throws Exception {
                        return tuple.f0.squaredDistance(tuple.f1);
                    }
                })
                .filter(new FilterFunction<Double>() {
                    @Override
                    public boolean filter(Double variance) throws Exception {
                        return variance > threshold * threshold;
                    }
                });

        // close cluster
        DataSet<ClusterCenter> finalClusters = iterativeCenters.closeWith(splittedCenters, convergenceSet);

        // map points to cluster
        DataSet<IndexedPoint> finalPoints = data
                .map(new Utils.SelectNearestCenter2()).withBroadcastSet(finalClusters, "centroids2");

        if (centerSummary != null) {
            finalClusters
                    .union(centerSummary)
                    .map(new Utils.UpdateRootForSplittedCentersMapFunc(center))
                    .writeAsCsv(indexDir, "\n", Constants.DELIMITER, FileSystem.WriteMode.OVERWRITE);
            env.execute("Execute Bisecting KMeans V11 divide data");
        } else {
            finalClusters
                    .map(new Utils.UpdateRootForSplittedCentersMapFunc(center))
                    .writeAsCsv(indexDir, "\n", Constants.DELIMITER, FileSystem.WriteMode.OVERWRITE);
            env.execute("Execute Bisecting KMeans V11 divide data");
        }

        return finalPoints;

    }

    public static ClusterCenter getRootCenter(int index) {

        try {
            ClusterCenter center = new ClusterCenter(index, null, true);
            // suppose that root node always have more than 2 rows to split
            center.setRow(2);
            return center;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }
}
