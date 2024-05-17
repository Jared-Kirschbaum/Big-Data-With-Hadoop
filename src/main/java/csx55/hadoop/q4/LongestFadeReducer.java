package csx55.hadoop.q4;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;


public class LongestFadeReducer extends Reducer<FloatWritable, Text, Text, FloatWritable> {
    private FloatWritable result = new FloatWritable();

    public void reduce(Text key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {
        float sum = 0;
        for (FloatWritable val : values) {
            sum += val.get();  // Summing all the float values received
        }
        result.set(sum);
        context.write(key, result);
    }
}