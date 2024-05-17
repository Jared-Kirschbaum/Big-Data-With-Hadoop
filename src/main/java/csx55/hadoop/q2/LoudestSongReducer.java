package csx55.hadoop.q2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;


public class LoudestSongReducer extends Reducer<Text, FloatWritable, Text, FloatWritable> {
    private FloatWritable result = new FloatWritable();

    public void reduce(Text key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {
        float sum = 0;
        int count = 0;
        for (FloatWritable val : values) {
            sum += val.get();
            count++;
        }
        if (count != 0) {
            result.set(sum / count);
            context.write(key, result);  // Outputs the artist ID and their average loudness
        }
    }
}
