package csx55.hadoop.q4;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;


public class MaxFadeReducer extends Reducer<FloatWritable, Text, Text, FloatWritable> {


    public void reduce(FloatWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
       
        for (Text value : values) {
            context.write(value, key);
        }
    }
}
