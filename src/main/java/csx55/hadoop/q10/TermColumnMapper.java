package csx55.hadoop.q10;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class TermColumnMapper extends Mapper<Object, Text, Text, Text> {
    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] parts = value.toString().split("\\|");

        Text songID = new Text(parts[0]); // Extracting the song ID.

        if (parts.length > 41) {
            String rawFeature = cleanText(parts[41]);
            if (rawFeature.isEmpty()) {  // Check if the cleaned text is empty
                rawFeature = "empty";  // Assign "empty" to rawFeature if it's empty
            }
            context.write(songID, new Text(rawFeature));
        }
    }


    private String cleanText(String text) {
        text = text.replaceAll("[`'\"\\[\\]\\{\\}]", "");  // Remove specific punctuation marks.
        text = text.replaceAll("\\s+", " ");  // Replace multiple spaces with a single space.
        text = text.toLowerCase();  // Convert everything to lower case to standardize the data.
        return text.trim();  // Trim leading and trailing spaces.
    }
}

