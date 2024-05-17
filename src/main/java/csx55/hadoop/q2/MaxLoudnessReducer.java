package csx55.hadoop.q2;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MaxLoudnessReducer extends Reducer<FloatWritable, Text, Text, Text> {

    @Override
    public void reduce(FloatWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // Since keys are sorted in descending order, the first value is the max count
            Text loudness = new Text(String.valueOf(key.get()));
            for (Text val : values) {
                context.write(val, loudness);  // Write out the artist ID and their song count
        }
    }
}
