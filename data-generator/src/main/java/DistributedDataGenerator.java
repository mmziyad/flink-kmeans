import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.TextOutputFormat;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Random;

/**
 * Created by JML on 12/5/16.
 * This class is inspired by https://github.com/stratosphere/stratosphere/blob/master/stratosphere-examples/stratosphere-java-examples/src/main/java/eu/stratosphere/example/java/clustering/util/KMeansDataGenerator.java
 *
 */
public class DistributedDataGenerator {
    static {
        Locale.setDefault(Locale.US);
    }

    private static final String CENTERS_FILE = "centers";
    private static final String POINTS_FILE = "points";
    private static final String POINTS_FILE_WITH_LABEL = "points_label";
    private static final long DEFAULT_SEED = 4650285087650871364L;
    private static final double RELATIVE_STDDEV = 0.05;
    private static final Integer DEFAULT_PARALLELISM = 4;
//    private static int dimensionality = 0;
    private static final DecimalFormat FORMAT = new DecimalFormat("#0");
    private static final char DELIMITER = ' ';

    /**
     * Main method to generate data for the {@link Kmeans} example program.
     * <p>
     * The generator creates to files:
     * <ul>
     * <li><code>{tmp.dir}/points</code> for the data points
     * <li><code>{tmp.dir}/centers</code> for the cluster centers
     * </ul>
     *
     * @param args
     * <ol>
     * <li>Int: Number of data points
     * <li>Int: Number of cluster centers
     * <li><b>Optional</b> Double: Standard deviation of data points
     * <li><b>Optional</b> Double: Value range of cluster centers
     * <li><b>Optional</b> Long: Random seed
     * </ol>
     */
    public static void main(String[] args) throws Exception {

        // check parameter count
        if (args.length < 5) {
            System.out.println("KMeansDataGenerator <output> <d> <size> <k> <minDistance> [<stddev>] [<seed>] <parallel>");
            System.exit(1);
        }

        //1. Get input parameters
        final ParameterTool params = ParameterTool.fromArgs(args);

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(params);

        // parse parameters
        final String outputDir = params.get("output");
        final int dimensionality = params.getInt("d");
        final long numDataPoints = params.getLong("size");
        final long minDistance = params.getLong("minDistance");
        final int k = params.getInt("k");
        final double stddev = params.getDouble("stddev", RELATIVE_STDDEV);
        final long firstSeed = params.getLong("seed", DEFAULT_SEED);
        final Integer parallelism = params.getInt("parallel", DEFAULT_PARALLELISM);

        // 2. Initialize standard deviation and random seed
        final double absoluteStdDev = stddev * minDistance;
        final Random random = new Random(firstSeed);

        // 3. Generate centers
        final long[][] centers = generateCenters(random, k, dimensionality, minDistance);

        // create centroid list for broadcastset
        List<Point> centroids = new ArrayList<Point>(centers.length);
        for(int i =0; i<centers.length; i++){
            Point point = new Point(dimensionality);
            for(int j=0; j<centers[i].length; j++){
//                System.out.print(centers[i][j] + " ");
                point.getFields()[j] = centers[i][j];
            }
//            System.out.println("");
            centroids.add(point);
        }
        DataSet<Point> centroidDataSet = env.fromCollection(centroids);

        // 4. Split the number of points need to be generated
        // calculate number of points to generate for each partition
        Long nbPointPerParallel = numDataPoints/ parallelism;
        List<Tuple4<Integer, Long, Double, Random>> partitionInfo = new ArrayList<Tuple4<Integer, Long, Double, Random>>(parallelism);
        for(int i =0; i<parallelism; i++){
            if(i != parallelism - 1){
                partitionInfo.add(new Tuple4<Integer, Long, Double, Random>(i, nbPointPerParallel, absoluteStdDev, random));
            }
            else{
                Long nbRemainedPoints = numDataPoints - nbPointPerParallel * (parallelism -1);
                partitionInfo.add(new Tuple4<Integer, Long, Double, Random>(i, nbRemainedPoints, absoluteStdDev, random));
            }
            System.out.println(partitionInfo.get(i));
        }

        // 5. Generate data points by broad cast centroids for each partition
        // then each partition will generate number of points (in partitionInfo) by standard deviation around each centroids
        DataSet<Point> points = env.fromCollection(partitionInfo)
                .partitionByHash(0)
                .flatMap(new GeneratePointsFlatMapFunc())
                .withBroadcastSet(centroidDataSet, "centroids");

        points.writeAsFormattedText(outputDir + "/" + POINTS_FILE, FileSystem.WriteMode.OVERWRITE, new TextOutputFormat.TextFormatter<Point>() {
            public String format(Point value) {
                return value.toString();
            }
        });

        System.out.println("Wrote "+numDataPoints+" data points to "+outputDir+"/" + POINTS_FILE);
        System.out.println("Wrote "+numDataPoints+" data points with labels to "+outputDir+"/"+POINTS_FILE_WITH_LABEL);
        System.out.println("Wrote "+k+" cluster centers to "+outputDir+"/"+CENTERS_FILE);

        env.execute("Distributed K-Means data generator");
    }


    private static final long[][] generateCenters(Random rnd, int k, int dimension, long minDistance) {
        final long[][] points = new long[k][dimension];
        int count =0;
        // initialize sum of all centers in each dimension
        long[] sumDimension = new long[dimension];
        for(int i =0; i< sumDimension.length; i++){
            sumDimension[i] = 0;
        }

        // start to generate centers
        // every time we get a new centers, we update the sumDimension
        // meanDimension = sumDimension/count
        // we will use meanDimension + variance to create a new centers
        while(count < k){
//            System.out.print("count" + count + ": ");
            for(int i =0; i< dimension; i++){
//                System.out.println("random: " + random);
                // 1. variance from mean of dimension = minimum distance + random value from (0 - half of minimum distance)
                // get random value between 0-1
                double random = rnd.nextGaussian();
                long variance = minDistance + (long) Math.floor(minDistance * random);

                // 2. Calculate mean of all current centers in dimension i
                long meanDimension = count == 0 ? sumDimension[i] : sumDimension[i]/(count);

                // 3. New point is calculate by variance distance from meanDimension
                points[count][i] = meanDimension + variance;
                sumDimension[i] = points[count][i];

            }
//            System.out.println("");
            count++;
        }

        return points;
    }

    public static double getRatio(long minDistance){
        double temp = System.nanoTime()/minDistance;
        long logTemp = (long)Math.log(temp);
        double result = Math.pow(10, logTemp/3);
        return result;
    }




}

