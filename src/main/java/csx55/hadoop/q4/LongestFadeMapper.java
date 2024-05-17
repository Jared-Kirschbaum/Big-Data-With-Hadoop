package csx55.hadoop.q4;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class LongestFadeMapper extends Mapper<Object, Text, Text, FloatWritable> {
    private FloatWritable songfadeIn = new FloatWritable();
    private Text artistId = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] parts = value.toString().split("\\|");
        if (parts.length > 20) {
            try {
                // Ensure loudness is not "NaN"; if it is, use 0.0f instead
                float fadeIn = "nan".equalsIgnoreCase(parts[5]) ? 0.0f : Float.parseFloat(parts[5]);
                songfadeIn.set(fadeIn);
                artistId.set(parts[33]);
                context.write(artistId, songfadeIn);  
            } catch (NumberFormatException e) {
                System.err.println("Skipping record with invalid number format for fadeIn: " + parts[5]);
            }
        }
    }
}


