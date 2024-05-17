package csx55.hadoop.q1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MaxSongReducer extends Reducer<IntWritable, Text, Text, IntWritable> {
    public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        // Since keys are sorted in descending order, the first value is the max count
        for (Text val : values) {
            context.write(val, key);  // Write out the artist ID and their song count
        }
    }
}
