package csx55.hadoop.q4;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class MaxFadeMapper extends Mapper<Object, Text, FloatWritable, Text> {
    private FloatWritable songfadeIn = new FloatWritable();
    private Text artistId = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] parts = value.toString().split("\\t");
        if (parts.length > 1) {
            try {
                // Ensure loudness is not "NaN"; if it is, use 0.0f instead
                float fadeIn = "nan".equalsIgnoreCase(parts[1]) ? 0.0f : Float.parseFloat(parts[1]);
                songfadeIn.set(fadeIn);
                artistId.set(parts[0]);  
                context.write(songfadeIn, artistId);  
            } catch (NumberFormatException e) {
                System.err.println("Skipping record with invalid number format for fadeIn: " + parts[1]);
            }
        }
    }
}


