package csx55.hadoop.q3;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class HottestSongMapper extends Mapper<Object, Text, FloatWritable, Text> {
    private FloatWritable songHottness = new FloatWritable();
    private Text songId = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] parts = value.toString().split("\\|");
        if (parts.length > 2) {
            float hotness = "nan".equalsIgnoreCase(parts[1]) ? 0.0f : Float.parseFloat(parts[1]);
            songHottness.set(hotness);
            songId.set(parts[0]);
            context.write(songHottness, songId);  // Key is hottness, value is song ID
        }
    }
}


