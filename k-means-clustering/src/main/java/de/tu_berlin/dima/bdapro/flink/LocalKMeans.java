package de.tu_berlin.dima.bdapro.flink;

import de.tu_berlin.dima.bdapro.datatype.Point;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

import java.util.*;

/**
 * Created by zis on 13/01/17.
 */

/**
 * Implementation of local kMeans using kMeans++ initialization.
 * called from InitKMeansParallel class
 * Adapted from Apache Spark Implementation: LocalKMeans.scala
 */
public class LocalKMeans implements GroupReduceFunction<Tuple3<Integer, Point, Long>, Point> {
    private int k;
    private int maxIter;
    private Random random;
    private int dimensions;

    protected LocalKMeans(int d, int k, int maxIter) {
        this.dimensions = d;
        this.k = k;
        this.maxIter = maxIter;
        this.random = new Random(Long.MAX_VALUE);
    }

    @Override
    public void reduce(Iterable<Tuple3<Integer, Point, Long>> iterable, Collector<Point> collector) throws Exception {
        // Identify initial k centres using kmeans ++
        List<Point> points = new ArrayList<>();
        List<Long> weights = new ArrayList<>();
        List<Double> costs = new ArrayList<>();

        Point centres[] = new Point[k];
        for (Tuple3 val : iterable) {
            points.add((Point) val.f1);
            weights.add((Long) val.f2);
        }

        Point initialCentre = getInitialCentre(points, weights);
        centres[0] = initialCentre;

        for (int i = 0; i < points.size(); i++) {
            costs.add(i, points.get(i).squaredDistance(centres[0]));
        }

        for (int i = 1; i < k; i++) {
            double sum = 0;
            for (int index = 0; index < points.size(); index++) {
                sum += weights.get(index) * costs.get(index);
            }
            double r = random.nextDouble() * sum;
            int j = 0;
            double cumulativeScore = 0;
            while (j < points.size() && cumulativeScore < r) {
                cumulativeScore += weights.get(j) * costs.get(j);
                j += 1;
            }
            if (j == 0) {
                centres[i] = points.get(0);
            } else {
                centres[i] = points.get(j - 1);
            }
            for (int index = 0; index < points.size(); index++) {
                costs.set(index, Math.min(points.get(index).squaredDistance(centres[i]), costs.get(index)));
            }
        }

        // Perform Lloyd's algorithm iterations
        for (int iteration = 0; iteration < maxIter; iteration++) {
            Map<Point, List<Point>> clusterMembers = new HashMap<>();
            for (Point centre : centres) {
                clusterMembers.put(centre, new ArrayList<>());
            }
            for (Point point : points) {
                double minDistance = Double.MAX_VALUE;
                Point correctCentre = null;
                for (Point centre : centres) {
                    double distance = point.squaredDistance(centre);
                    if (distance < minDistance) {
                        minDistance = distance;
                        correctCentre = centre;
                    }
                }
                clusterMembers.get(correctCentre).add(point);
            }


            for (int i = 0; i < k; i++) {
                centres[i] = findCentroidOfPoints(clusterMembers.get(centres[i]), dimensions);

            }
        }
        // The updated centres
        for (Point centre : centres) {
            collector.collect(centre);
        }
    }

    /**
     * Find the centroid of given Points
     */
    private Point findCentroidOfPoints(List<Point> points, int dimensions) {
        Point centroid = new Point(dimensions);
        for (Point p : points) {
            centroid.add(p);
        }
        return centroid.divideByScalar(points.size());
    }

    /**
     * Get the initial centre from the weighted points
     */
    private Point getInitialCentre(List<Point> points, List<Long> weights) {
        long sumOfWeights = 0;
        for (long weight : weights) sumOfWeights += weight;
        double r = random.nextDouble() * sumOfWeights;
        int i = 0;
        double cureWeight = 0;
        while (i < points.size() && cureWeight < r) {
            cureWeight += weights.get(i);
            i += 1;
        }
        return points.get(i - 1);
    }
}
