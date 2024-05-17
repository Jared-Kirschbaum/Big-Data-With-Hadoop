package csx55.hadoop.q9;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;


public class GlobalTopTermsReducer extends Reducer<IntWritable, Text, Text, IntWritable> {

    @Override
    public void reduce(IntWritable key, Iterable<Text> values, Context context) 
            throws IOException, InterruptedException {
        // Since multiple terms can have the same count, we process all for a given key
        for (Text value : values) {
            context.write(value, key);  // Output each term with its count
        }
    }
}

