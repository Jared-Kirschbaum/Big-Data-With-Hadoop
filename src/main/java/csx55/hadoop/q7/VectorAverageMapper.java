package csx55.hadoop.q7;


import java.io.*;
import java.net.URI;
import java.util.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class VectorAverageMapper extends Mapper<Object, Text, Text, Text> {
    private Map<String, Integer> maxLengths = new HashMap<>();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        // Reading the max lengths file from the Distributed Cache
        URI[] cacheFiles = context.getCacheFiles();  // Get the cached files
        if (cacheFiles != null && cacheFiles.length > 0) {
            FileSystem fs = FileSystem.get(context.getConfiguration());
            Path getPath = new Path(cacheFiles[0]); // Path to the symlink created for the max lengths file
            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(fs.open(getPath)));
            String line = null;
            while ((line = bufferedReader.readLine()) != null) {
                String[] parts = line.split("\\t");
                if (parts.length == 2) {
                    maxLengths.put(parts[0], Integer.parseInt(parts[1]));
                }
            }
            bufferedReader.close();
        }
    }

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        // Parse each line and write the output as <column_name, extended_vector>
        String[] parts = value.toString().split("\\t");
        if (parts.length == 2) {
            String columnKey = parts[0];
            String[] vectorParts = parts[1].split("\\s+");
            Integer maxLength = maxLengths.getOrDefault(columnKey, vectorParts.length);
            StringBuilder sb = new StringBuilder();
            
            // Extend the vector to the maximum length
            for (int i = 0; i < maxLength; i++) {
                if (i < vectorParts.length) {
                    sb.append(vectorParts[i]).append(" ");
                } else {
                    sb.append("0 ").append(" ");  // Filling with zeroes
                }
            }
            context.write(new Text(columnKey), new Text(sb.toString().trim()));
        }
    }
}
