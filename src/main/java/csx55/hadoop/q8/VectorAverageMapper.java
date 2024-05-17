package csx55.hadoop.q8;

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
            String line;
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
        // Parse each line and write the output as <authorId|column_name, extended_vector>
        String fullKey = value.toString().split("\\t", 2)[0]; // The full key from input which includes authorId and column name
        String vectorData = value.toString().split("\\t", 2)[1]; // The vector data

        String[] keyParts = fullKey.split("\\|", -1); // Correct split to parse authorId and columnName
        if (keyParts.length >= 2) {
            String authorId = keyParts[0];  // authorId
            String columnName = keyParts[1]; // column name

            String[] vectorParts = vectorData.split("\\s+"); // Splitting spaces for vector parts
            Integer maxLength = maxLengths.getOrDefault(columnName, vectorParts.length); // Use column name to get maxLength
            StringBuilder sb = new StringBuilder();
            
            // Extend the vector to the maximum length specified for this column
            for (int i = 0; i < maxLength; i++) {
                if (i < vectorParts.length) {
                    sb.append(vectorParts[i]).append(" ");
                } else {
                    sb.append("0 ");  // Filling with zeroes if short
                }
            }
            
            context.write(new Text(authorId + "|" + columnName), new Text(sb.toString().trim()));  // Keep the full key format
        }
    }
}
