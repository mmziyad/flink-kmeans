import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;

/**
 * Created by zis on 12/03/17.
 */
public class HiggsDataPreprocessor {

    public static void main(String[] args) throws Exception {
        // Checking input parameters
        final ParameterTool params = ParameterTool.fromArgs(args);
        if (!params.has("input"))
            throw new Exception("Input Data is not specified");

        if (!params.has("features"))
            throw new Exception("Features to include is not specified: Specify '--features [all|main]' ");

        // set up execution environment
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(params);

        // whether all features should be included or
        String features = params.get("features");
        int dimensions;
        if (features.equals("all")) dimensions = 28;
        else if (features.equals("main")) dimensions = 21;
        else throw new IllegalArgumentException("Invalid feature description");

        // get input data:
        DataSet<Point> points = env.readTextFile(params.get("input"))
                .map(new HiggsData(dimensions));

        // format the results
        // DataSet<String> formattedResult = points.map(new ResultFormatter());

        // emit result
        if (params.has("output")) {
            //finalCentroids.writeAsCsv(params.get("output"), "\n", Constants.DELIMITER, FileSystem.WriteMode.OVERWRITE);
            points.writeAsText(params.get("output"), FileSystem.WriteMode.OVERWRITE);
            // since file sinks are lazy, we trigger the execution explicitly
            env.execute("Higgs Data Preprocessing");
        } else {
            System.out.println("Printing result to stdout. Use --output to specify output path.");
            //finalCentroids.print();
            points.print();
        }
    }

    private static class HiggsData implements MapFunction<String, Point> {
        double[] data;

        public HiggsData(int dimensions) {
            data = new double[dimensions];
        }

        public Point map(String value) throws Exception {
            String fields[] = value.split(",");
            for (int i = 1; i <= (data.length ); i++) {
                data[i-1] = Double.valueOf(fields[i]);
            }
            return new Point(data);
        }
    }
}
