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
        if (inputFile == null || k == null || iterations == null || outputDir == null || dimension == null) {
            System.out.println("BisectingKMeans <input> <output> <dimension> <k> <iterations> [<threshold>]");
            System.exit(1);
        }

        // set up execution environment
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(params);

        // get input data:
        DataSet<IndexedPoint> dataPoints = env.readTextFile(params.get("input"))
                .map(new UDFs.PointData(dimension))
                .map(new MapFunction<Point, IndexedPoint>() {
                    @Override
                    public IndexedPoint map(Point point) throws Exception {
                        return new IndexedPoint(1, point);
                    }
                });

        // calculate root center
        ClusterCenter rootNode = getRootCenter(1, dataPoints);
//        System.out.println("Root node: " + rootNode.toString());

        int loop = 1;
        // start with root node
        Map<Integer, ClusterCenter> indexTree = new HashedMap();
        indexTree.put(rootNode.getIndex(), rootNode);

        // loop until get k
//        IterativeDataSet<IndexedPoint> iterativeData = dataPoints.iterate(k-1);
        while (loop < k) {
            // calculate the leaf node to split
            // -> maybe send by broadcast set
            final ClusterCenter dividableLeafNode = getDividableLeafNode(indexTree);

            // filter data of chosen leaf node
            DataSet<IndexedPoint> dividableData = dataPoints.filter(new FilterFunction<IndexedPoint>() {
                @Override
                public boolean filter(IndexedPoint point) throws Exception {
                    return dividableLeafNode.getIndex() == point.getIndex();
                }
            });

            // filter out data of other leaf nodes
            DataSet<IndexedPoint> undividableData = dataPoints.filter(new FilterFunction<IndexedPoint>() {
                @Override
                public boolean filter(IndexedPoint point) throws Exception {
                    return dividableLeafNode.getIndex() != point.getIndex();
                }
            });

            // divide data then union with undivided data
            DataSet<IndexedPoint> result = divideData(dividableData, dividableLeafNode, iterations, threshold, indexTree)
                    .union(undividableData);
            dataPoints = result;
            loop++;
        }
//        System.out.println("Final result: ");
//        dataPoints.print();
//        System.out.println("-----Final result: ");

        // emit result
        if (outputDir != null || !outputDir.isEmpty()) {
            //finalCentroids.writeAsCsv(params.get("output"), "\n", Constants.DELIMITER, FileSystem.WriteMode.OVERWRITE);
            dataPoints.map(new MapFunction<IndexedPoint, Tuple2<Integer, Point>>() {
                @Override
                public Tuple2<Integer, Point> map(IndexedPoint indexedPoint) throws Exception {
                    return new Tuple2<Integer, Point>(indexedPoint.getIndex(), indexedPoint.getPoint());
                }
            })
                    .writeAsCsv(outputDir, "\n", Constants.DELIMITER, FileSystem.WriteMode.OVERWRITE);
            // since file sinks are lazy, we trigger the execution explicitly
            env.execute("Bisecting Kmeans Clustering");
        } else {
            System.out.println("Printing result to stdout. Use --output to specify output path.");
            //finalCentroids.print();
            dataPoints.print();
        }

    }

    public static ClusterCenter getDividableLeafNode(Map<Integer, ClusterCenter> indexTree) {
        Set<Integer> keySet = indexTree.keySet();
        long maxSize = 0;
        int indexOfMaxNode = 0;
        Iterator<Integer> keyIter = keySet.iterator();
        while (keyIter.hasNext()) {
            Integer index = keyIter.next();
            ClusterCenter node = indexTree.get(index);
            if (node.getRow() > maxSize && node.getRow() >= 2 && node.isLeafNode() == true) {
                maxSize = node.getRow();
                indexOfMaxNode = index;
            }
        }
        return indexTree.get(indexOfMaxNode);
    }

    public static DataSet<IndexedPoint> divideData(DataSet<IndexedPoint> data, final ClusterCenter center, int maxIterations, final float threshold, final Map<Integer, ClusterCenter> indexTree) throws Exception {
        // pick random initial centers to split
        DataSet<ClusterCenter> initialCenters = DataSetUtils.sampleWithSize(data, false, 2, Long.MAX_VALUE)
                .reduceGroup(new GroupReduceFunction<IndexedPoint, ClusterCenter>() {
                    @Override
                    public void reduce(Iterable<IndexedPoint> iterable, Collector<ClusterCenter> collector) throws Exception {
                        Iterator<IndexedPoint> iterator = iterable.iterator();
                        int i = 0;
                        while (iterator.hasNext()) {
                            IndexedPoint pointIdx = iterator.next();
                            collector.collect(new ClusterCenter(pointIdx.getIndex() * 2 + i, pointIdx.getPoint()));
                            i++;
                        }
                    }
                });

        // iterate maxIterations except convergence condition is met
        IterativeDataSet<ClusterCenter> iterativeCenters = initialCenters.iterate(maxIterations);

        // get closet center
        DataSet<Tuple3<Integer, IndexedPoint, Long>> mappedData = data.map(new Utils.SelectNearestCenter()).withBroadcastSet(iterativeCenters, "centroids");

        // sum all points and calculate new centers
        DataSet<ClusterCenter> splittedCenters = mappedData
                .groupBy(0)
                .reduce(new Utils.SumClusterPointsReduceFunc())
                .map(new Utils.MapSumToClusterCenterFunc());

        // check convergence with previous centers
        JoinOperator.DefaultJoin<ClusterCenter, ClusterCenter> joinedCenters = iterativeCenters.join(splittedCenters).where(new KeySelector<ClusterCenter, Integer>() {
            @Override
            public Integer getKey(ClusterCenter clusterCenter) throws Exception {
                return clusterCenter.getIndex();
            }
        }).equalTo(new KeySelector<ClusterCenter, Integer>() {
            @Override
            public Integer getKey(ClusterCenter clusterCenter) throws Exception {
                return clusterCenter.getIndex();
            }
        });

        DataSet<Double> convergenceSet = joinedCenters.map(new MapFunction<Tuple2<ClusterCenter, ClusterCenter>, Double>() {

            @Override
            public Double map(Tuple2<ClusterCenter, ClusterCenter> tuple) throws Exception {
                return Math.sqrt(tuple.f0.getPoint().squaredDistance(tuple.f1.getPoint()));
            }
        }).filter(new FilterFunction<Double>() {
            @Override
            public boolean filter(Double variance) throws Exception {
                return variance > threshold;
            }
        });


        DataSet<ClusterCenter> finalClusters = iterativeCenters.closeWith(splittedCenters, convergenceSet);

        DataSet<IndexedPoint> finalPoints = data.map(new Utils.SelectNearestCenter()).withBroadcastSet(finalClusters, "centroids")
                .map(new MapFunction<Tuple3<Integer, IndexedPoint, Long>, IndexedPoint>() {
                    @Override
                    public IndexedPoint map(Tuple3<Integer, IndexedPoint, Long> tuple) throws Exception {
                        return tuple.f1;
                    }
                });

        List<ClusterCenter> centerList = finalClusters.collect();
        for (int i = 0; i < centerList.size(); i++) {
            ClusterCenter item = centerList.get(i);
            indexTree.put(item.getIndex(), item);
        }

        // reset leaf node of the original center after dividing
        center.setLeafNode(false);
//        System.out.println("DivideData root node: " + indexTree.toString());
//
//        System.out.println("Final center: ");
//        finalClusters.print();
//        System.out.println("----------- Final center: ");

        return finalPoints;

    }

    public static ClusterCenter getRootCenter(int index, DataSet<IndexedPoint> data) {

        try {
            List<ClusterCenter> centerList = data.map(new MapFunction<IndexedPoint, Tuple3<Integer, Point, Long>>() {
                @Override
                public Tuple3<Integer, Point, Long> map(IndexedPoint pointIndex) throws Exception {
                    return new Tuple3<Integer, Point, Long>(1, pointIndex.getPoint(), 1L);
                }
            })
                    .reduce(new ReduceFunction<Tuple3<Integer, Point, Long>>() {
                        @Override
                        public Tuple3<Integer, Point, Long> reduce(Tuple3<Integer, Point, Long> sumPoint, Tuple3<Integer, Point, Long> t1) throws Exception {
                            return new Tuple3<Integer, Point, Long>(sumPoint.f0, sumPoint.f1.add(t1.f1), sumPoint.f2 + t1.f2);
                        }
                    })
                    .map(new MapFunction<Tuple3<Integer, Point, Long>, ClusterCenter>() {
                        @Override
                        public ClusterCenter map(Tuple3<Integer, Point, Long> tuple) throws Exception {
                            ClusterCenter result = new ClusterCenter(tuple.f0, tuple.f1.divideByScalar(tuple.f2), true);
                            result.setRow(tuple.f2);
                            return result;
                        }
                    }).collect();
            return centerList.get(0);
//            double[] point = new double[2];
//            point[0] = 0;
//            point[1] = 0;
//            ClusterCenter center =  new ClusterCenter(1,new Point(point), true);
//            center.setRow(25);
//            return center;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }


}
