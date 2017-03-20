import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.Locale;
import java.util.Random;

/**
 * Created by JML on 12/5/16.
 * This class is inspired by https://github.com/stratosphere/stratosphere/blob/master/stratosphere-examples/stratosphere-java-examples/src/main/java/eu/stratosphere/example/java/clustering/util/KMeansDataGenerator.java
 *
 */
public class KMeansDataGenerator {
    static {
        Locale.setDefault(Locale.US);
    }

    private static final String CENTERS_FILE = "centers";
    private static final String POINTS_FILE = "points";
    private static final String POINTS_FILE_WITH_LABEL = "points_label";
    private static final long DEFAULT_SEED = 4650285087650871364L;
    private static final double DEFAULT_VALUE_RANGE = 100.0;
    private static final double RELATIVE_STDDEV = 0.08;
//    private static int dimensionality = 0;
    private static final DecimalFormat FORMAT = new DecimalFormat("#0.00");
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
    public static void main(String[] args) throws IOException {

        // check parameter count
        if (args.length < 2) {
            System.out.println("KMeansDataGenerator <outputDir> <numberOfDimensions> <numberOfDataPoints> <numberOfClusterCenters> [<relative stddev>] [<centroid range>] [<seed>]");
            System.exit(1);
        }

        // parse parameters
        final String outputDir = args[0];
        final int dimensionality = Integer.parseInt(args[1]);
        final int numDataPoints = Integer.parseInt(args[2]);
        final int k = Integer.parseInt(args[3]);
        final double stddev = args.length > 4 ? Double.parseDouble(args[4]) : RELATIVE_STDDEV;
        final double range = args.length > 5 ? Double.parseDouble(args[5]) : DEFAULT_VALUE_RANGE;
        final long firstSeed = args.length > 6 ? Long.parseLong(args[6]) : DEFAULT_SEED;


        final double absoluteStdDev = stddev * range;
        final Random random = new Random(firstSeed);

        // the means around which data points are distributed
        final double[][] means = uniformRandomCenters(random, k, dimensionality, range);

        // write the points out
        BufferedWriter pointsOut = null;
        BufferedWriter pointsLabelOut = null;
        try {
            pointsOut = new BufferedWriter(new FileWriter(new File(outputDir + "/" + POINTS_FILE)));
            pointsLabelOut = new BufferedWriter(new FileWriter(new File(outputDir + "/" + POINTS_FILE_WITH_LABEL)));
            StringBuilder buffer = new StringBuilder();

            double[] point = new double[dimensionality];
            int nextCentroid = 0;

            for (int i = 1; i <= numDataPoints; i++) {
                // generate a point for the current centroid
                double[] centroid = means[nextCentroid];
                for (int d = 0; d < dimensionality; d++) {
                    point[d] = (random.nextGaussian() * absoluteStdDev) + centroid[d];
                }
                writePoint(0, false, point, buffer, pointsOut);
                // write another file with points and its label (or cluster)
                writePoint(nextCentroid, true, point, buffer, pointsLabelOut);
                nextCentroid = (nextCentroid + 1) % k;
            }
        }
        finally {
            if (pointsOut != null) {
                pointsOut.close();
            }

            if (pointsLabelOut != null) {
                pointsLabelOut.close();
            }

        }

        // write the uniformly distributed centers to a file
        BufferedWriter centersOut = null;
        try {
            centersOut = new BufferedWriter(new FileWriter(new File(outputDir+"/"+CENTERS_FILE)));
            StringBuilder buffer = new StringBuilder();

            double[][] centers = uniformRandomCenters(random, k, dimensionality, range);

            for (int i = 0; i < k; i++) {
                writeCenter(i + 1, centers[i], buffer, centersOut);
            }
        }
        finally {
            if (centersOut != null) {
                centersOut.close();
            }
        }

        System.out.println("Wrote "+numDataPoints+" data points to "+outputDir+"/"+POINTS_FILE);
        System.out.println("Wrote "+numDataPoints+" data points with labels to "+outputDir+"/"+POINTS_FILE_WITH_LABEL);
        System.out.println("Wrote "+k+" cluster centers to "+outputDir+"/"+CENTERS_FILE);
    }

    private static final double[][] uniformRandomCenters(Random rnd, int num, int dimensionality, double range) {
        final double halfRange = range / 2;
        final double[][] points = new double[num][dimensionality];

        for (int i = 0; i < num; i++) {
            for (int dim = 0; dim < dimensionality; dim ++) {
                double x = rnd.nextDouble();
//                System.out.println("Db: " + x);
                points[i][dim] = (x * range) - halfRange;
            }
        }
        return points;
    }

    private static void writePoint(int cluster, boolean isLabeled, double[] coordinates, StringBuilder buffer, BufferedWriter out) throws IOException {
        buffer.setLength(0);

        if(isLabeled){
            buffer.append(cluster);
            buffer.append(DELIMITER);
        }

        // write coordinates
        for (int j = 0; j < coordinates.length; j++) {
            buffer.append(FORMAT.format(coordinates[j]));
            if(j < coordinates.length - 1) {
                buffer.append(DELIMITER);
            }
        }

        out.write(buffer.toString());
        out.newLine();
    }

    private static void writeCenter(long id, double[] coordinates, StringBuilder buffer, BufferedWriter out) throws IOException {
        buffer.setLength(0);

        // write id
        buffer.append(id);
        buffer.append(DELIMITER);

        // write coordinates
        for (int j = 0; j < coordinates.length; j++) {
            buffer.append(FORMAT.format(coordinates[j]));
            if(j < coordinates.length - 1) {
                buffer.append(DELIMITER);
            }
        }

        out.write(buffer.toString());
        out.newLine();
    }
}
