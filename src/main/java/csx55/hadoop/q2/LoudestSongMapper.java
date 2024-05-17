package csx55.hadoop.q2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class LoudestSongMapper extends Mapper<Object, Text, Text, FloatWritable> {
    private Text artistId = new Text();
    private FloatWritable loudness = new FloatWritable();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] parts = value.toString().split("\\|");
        if (parts.length > 33 && parts[9] != null && !parts[9].isEmpty()) {
            artistId.set(parts[33]);  // Assuming artist_id is at index 33
            loudness.set(Float.parseFloat(parts[9]));  // Assuming loudness value is at index 9
            context.write(artistId, loudness);
        }
    }
}