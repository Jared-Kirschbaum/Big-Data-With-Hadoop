package csx55.hadoop.q5;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class SongMapper extends Mapper<Object, Text, FloatWritable, Text> {
    private FloatWritable duration = new FloatWritable();
    private Text songId = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] parts = value.toString().split("\\|");
        if (parts.length > 4) {
            try {
                float dur = Float.parseFloat(parts[4]);
                duration.set(dur);
                songId.set(parts[0]);
                context.write(duration, songId);
            } catch (NumberFormatException e) {
                // Handle parse error or ignore the record
            }
        }
    }
}

