package de.tu_berlin.dima.bdapro.flink;

import org.apache.flink.ml.math.Vector;

import java.io.*;

/**
 * Created by JML on 12/10/16.
 */
public final class Utils {

    public static void mergeFile(String inputDir, String outputFile) throws Exception{
        File dir = new File(inputDir);
        if(dir.isDirectory()){
            File[] files = dir.listFiles();
            BufferedWriter outputWriter = new BufferedWriter(new FileWriter(new File(outputFile)));
            for(int i =0; i<files.length; i++){
                BufferedReader inputReader = new BufferedReader(new FileReader(files[i]));
                String line = inputReader.readLine();
                while (line != null){
                    outputWriter.write(line + "\n");
                    line = inputReader.readLine();
                }
                inputReader.close();
            }
            outputWriter.close();
        }
    }

    public static String vectorToCustomString(Vector vector){
        StringBuilder result = new StringBuilder();
        for (int i = 0; i < vector.size(); i++) {
            result.append(vector.apply(i));
            result.append(" ");
        }
        return result.toString();
    }



}
