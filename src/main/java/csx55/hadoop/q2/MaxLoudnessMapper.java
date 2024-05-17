package csx55.hadoop.q2;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MaxLoudnessMapper extends Mapper<Object, Text, FloatWritable, Text> {
    private FloatWritable loudness = new FloatWritable();
    private Text artistId = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] parts = value.toString().split("\\t");
        if (parts.length == 2) {
            loudness.set(Float.parseFloat(parts[1]));
            artistId.set(parts[0]);

            // swapping so loudness is the key and artistId is the value to handle distributed reducer
            context.write(loudness, artistId);
        }
    }
}