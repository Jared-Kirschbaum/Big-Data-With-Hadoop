package csx55.hadoop.q6;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class EnergyDanceMapper extends Mapper<Object, Text, FloatWritable, Text> {
   private FloatWritable combinedScore = new FloatWritable();
    private Text songId = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] parts = value.toString().split("\\|");
        if (parts.length > 20) { 
            float energy = Float.parseFloat(parts[6]); 
            float danceability = Float.parseFloat(parts[3]); 
            songId.set(parts[0]); 

            // Simple average or any other combination formula
            float score = (energy + danceability) / 2.0f;
            combinedScore.set(score);

            // Emit combined score as key for sorting
            context.write(combinedScore, songId);
        }
    }
}
