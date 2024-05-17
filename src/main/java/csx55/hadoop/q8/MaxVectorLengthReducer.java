package csx55.hadoop.q8;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MaxVectorLengthReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int maxLength = 0;
        for (IntWritable val : values) {
            maxLength = Math.max(maxLength, val.get());
        }
        result.set(maxLength);
        context.write(key, result);
    }
}
