package csx55.hadoop.q6;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

public class TopSongsReducer extends Reducer<FloatWritable, Text, Text, FloatWritable> {
    
    private int numSongs = 0;

    public void reduce(FloatWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text value : values) {
            if (numSongs < 10) {
                context.write(value, key);
                numSongs++;
            } else {
                break; // Exit after the top 10 are reached
            }
        }
    }
}

