package csx55.hadoop.q10;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class TFIDFMapper extends Mapper<Object, Text, Text, Text> {
    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        // Assuming input format is "term^songId    term_count"
        String[] parts = value.toString().split("\\t");
        if (parts.length == 2) {
            String termSongId = parts[0];  // This is "term^songId"
            String termCount = parts[1];   // This is "term_count"

            context.write(new Text(termSongId), new Text(termCount));
        } else {
            // Log error or throw an exception if the format is not as expected
            System.err.println("Invalid input format: " + value.toString());
        }
    }
}
