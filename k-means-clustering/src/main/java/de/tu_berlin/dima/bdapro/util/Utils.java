package de.tu_berlin.dima.bdapro.util;

import de.tu_berlin.dima.bdapro.datatype.Point;

import java.io.*;

/**
 * Created by JML on 12/10/16.
 */
public class Utils {

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

    public static String vectorToCustomString(Point vector){
        StringBuilder result = new StringBuilder();
        for (int i = 0; i < vector.getFields().length; i++) {
            result.append(vector.getFields()[i]);
            result.append(" ");
        }
        return result.toString();
    }



}
