package csx55.hadoop.q8;

import java.io.*;
import java.net.URI;
import java.util.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class DistanceMapper extends Mapper<Text, Text, Text, DoubleWritable> {
    private Map<String, double[]> averageVectors = new HashMap<>();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        // Setup to read the distributed cache with average vectors
        URI[] cacheFiles = context.getCacheFiles();
        if (cacheFiles != null && cacheFiles.length > 0) {
            FileSystem fs = FileSystem.get(context.getConfiguration());
            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(new Path(cacheFiles[0]))));
            String line;
            while ((line = br.readLine()) != null) {
                String[] parts = line.split("\t");
                if (parts.length == 2) {
                    // Storing average vectors by column name
                    averageVectors.put(parts[0].trim(), Arrays.stream(parts[1].split("\\s+"))
                                                              .mapToDouble(Double::parseDouble)
                                                              .toArray());
                }
            }
            br.close();
        }
    }

    @Override
    public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        String[] keyParts = key.toString().split("\\|");
        if (keyParts.length != 2) {
            System.err.println("Skipping record due to incorrect key formatting: " + key.toString());
            return; // Skip this record
        }

        String artistId = keyParts[0].trim();
        String columnName = keyParts[1].trim();
        double[] artistVector = Arrays.stream(value.toString().trim().split("\\s+"))
                                      .mapToDouble(Double::parseDouble)
                                      .toArray();

        double[] avgVector = averageVectors.get(columnName);
        if (avgVector == null) {
            System.err.println("No average vector found for column: " + columnName);
            return; // Skip this record
        }

        double distance = euclideanDistance(artistVector, avgVector);
        context.write(new Text(artistId), new DoubleWritable(distance));
    }

    private double euclideanDistance(double[] vector1, double[] vector2) {
        double sum = 0.0;
        for (int i = 0; i < vector1.length; i++) {
            sum += Math.pow(vector1[i] - vector2[i], 2);
        }
        return Math.sqrt(sum);
    }
}


