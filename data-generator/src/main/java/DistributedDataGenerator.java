import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.TextOutputFormat;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Random;

/**
 * Created by JML on 12/5/16.
 * This class is inspired by:
 * https://github.com/stratosphere/stratosphere/blob/master/stratosphere-examples/stratosphere-java-examples/src/main/java/eu/stratosphere/example/java/clustering/util/KMeansDataGenerator.java
 */
public class DistributedDataGenerator {
    private static final long DEFAULT_SEED = 4650285087651364L;
    private static final double RELATIVE_STDDEV = 0.01;
    private static final Integer DEFAULT_PARALLELISM = 10;
    private static final double DEFAULT_NOISE_DEVIATION = 0.5;
    private static final double DEFAULT_NOISE_FRACTION = 0.1;

    static {
        Locale.setDefault(Locale.US);
    }

    /**
     * Main method to generate data for the {@link Kmeans} example program.
     * <p>
     * Algorithms:
     * Phase 1: generate initial centers
     * a. Pick one point first and add to center set S
     * b. Create next points by
     * i. For each dimension ith, get mean of all points in dimension ith in S, new points will be calculated by mean of dimension ith + variance
     * ii. Variance = minDistance + (minDistance * rnd.nextGaussain)
     * c. Add new points to S
     * d. Loop from step b until we get enough initial centers
     * Phase 2: From initial centers, generate points around this centers by Gaussian distribution
     * </p>
     * <p>
     * With minDistance is set too big, remember to reduce standard deviation to keep the data points are sparse
     * With initial cluster center is so closed to each other, try changing default seed to make different solutions
     * </p>
     *
     * @param args <ol>
     *             <li>output: Output file location
     *             <li>d: Number of dimensions
     *             <li>size: Number of data points
     *             <li>k: Number of cluster
     *             <li>minDistance: Minimum distance from mean of all centers
     *             <li><b>Optional</b> stddev: Standard deviation of data points
     *             <li><b>Optional</b> seed: Random seed
     *             <li><b>Optional</b> parallel: parallelism should we split
     *             </ol>
     */
    public static void main(String[] args) throws Exception {
        System.out.println("KMeansDataGenerator <output> <d> <size> <k> <minDistanceFromMeanCenter> [<stddev>] [<seed>] [<parallel>] [<noisedev>] [<noisefrac>]");
        System.out.println("     - output: Output file location\n" +
                "     - d: Number of dimensions\n" +
                "     - size: Number of data points\n" +
                "     - k: Number of clusters\n" +
                "     - minDistance: Minimum distance from mean of all centers\n" +
                "     (Optional) stddev: Standard deviation of data points\n" +
                "     (Optional) seed: Random seed\n" +
                "     (Optional) parallel: parallelism should we split\n" +
                "     (Optional) noisedev: Standard deviation of data noise\n" +
                "     (Optional) noisefrac: Fraction of data points which are considered as noise\n");

        //1. Get input parameters
        final ParameterTool params = ParameterTool.fromArgs(args);

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(params);

        // parse parameters
        final String outputDir = params.get("output");
        final Integer dimension = params.getInt("d");
        final Long numDataPoints = params.getLong("size");
        final Long minDistance = params.getLong("minDistance");
        final Integer k = params.getInt("k");
        final Double stddev = params.getDouble("stddev", RELATIVE_STDDEV);
        final Long firstSeed = params.getLong("seed", DEFAULT_SEED);
        final Integer parallelism = params.getInt("parallel", DEFAULT_PARALLELISM);
        final Double noisedev = params.getDouble("noisedev", DEFAULT_NOISE_DEVIATION);
        final Double noisefrac = params.getDouble("noisefrac", DEFAULT_NOISE_FRACTION);

        // 2. Initialize standard deviation and random seed
        final double absoluteStdDev = stddev * minDistance;
        final Random random = new Random(firstSeed);
        final double noiseStdDev = noisedev * minDistance;

        // 3. Generate centers
        final long[][] centers = generateCenters(random, k, dimension, minDistance);

        // create centroid list for broadcast set
        List<Point> centroids = new ArrayList<Point>(centers.length);
        for (int i = 0; i < centers.length; i++) {
            Point point = new Point(dimension);
            for (int j = 0; j < centers[i].length; j++) {
                point.getFields()[j] = centers[i][j];
            }
            centroids.add(point);
        }
        DataSet<Point> centroidDataSet = env.fromCollection(centroids);

        // 4. Split the number of points need to be generated
        // calculate number of points to generate for each partition
        Long nbPointPerParallel = numDataPoints / parallelism;
        List<Tuple6<Integer, Long, Double, Random, Double, Double>> partitionInfo = new ArrayList<Tuple6<Integer, Long, Double, Random, Double, Double>>(parallelism);
        for (int i = 0; i < parallelism; i++) {
            if (i != parallelism - 1) {
                partitionInfo.add(new Tuple6<Integer, Long, Double, Random, Double, Double>(i, nbPointPerParallel, absoluteStdDev, random, noiseStdDev, noisefrac));
            } else {
                Long nbRemainedPoints = numDataPoints - nbPointPerParallel * (parallelism - 1);
                partitionInfo.add(new Tuple6<Integer, Long, Double, Random, Double, Double>(i, nbRemainedPoints, absoluteStdDev, random, noiseStdDev, noisefrac));
            }
        }



        // 5. Generate data points by broadcasting centroids for each partition
        // then each partition will generate number of points (in partitionInfo) by standard deviation around each centroids
        DataSet<Point> points = env.fromCollection(partitionInfo)
                .partitionByHash(0)
                .flatMap(new GeneratePointsFlatMapFunc())
                .withBroadcastSet(centroidDataSet, "centroids");

        // 6. write the result
        points.writeAsFormattedText(outputDir, FileSystem.WriteMode.OVERWRITE, new TextOutputFormat.TextFormatter<Point>() {
            public String format(Point value) {
                return value.toString();
            }
        });
        env.execute("Distributed K-Means data generator");
    }


    private static final long[][] generateCenters(Random rnd, int k, int dimension, long minDistance) {
        final long[][] points = new long[k][dimension];
        int count = 0;
        // initialize sum of all centers in each dimension
        long[] sumDimension = new long[dimension];
        for (int i = 0; i < sumDimension.length; i++) {
            sumDimension[i] = 0;
        }

        // start to generate centers
        // every time we get a new centers, we update the sumDimension
        // meanDimension = sumDimension/count
        // we will use meanDimension + variance to create a new centers
        while (count < k) {
            for (int i = 0; i < dimension; i++) {
                // 1. variance from mean of dimension = minimum distance + random value from ((-minDistance) - (minDistance))
                // get random value between 0-1
                double random = rnd.nextGaussian();
                long variance = minDistance + (long) Math.floor(minDistance * random);

                // 2. Calculate mean of all current centers in dimension i
                long meanDimension = count == 0 ? sumDimension[i] : sumDimension[i] / (count);

                // 3. New point is calculated by variance distance from meanDimension
                points[count][i] = meanDimension + variance;

                sumDimension[i] = sumDimension[i] + points[count][i];
            }
            count++;
        }

        return points;
    }

    public static boolean checkDistance(long[][] points, int count, int dimension, long newPoint, long minDistance) {
        for (int i = 0; i < count; i++) {
            if (Math.abs(newPoint - points[i][dimension]) < minDistance) {
                return false;
            }
        }
        return true;
    }
}
