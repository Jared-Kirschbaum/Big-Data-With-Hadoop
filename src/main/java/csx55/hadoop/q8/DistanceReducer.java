package csx55.hadoop.q8;

import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class DistanceReducer extends Reducer<Text, DoubleWritable, Text, Text> {

    private Text mostUniqueArtist = new Text("Most Unique Artist: ");
    private Text mostAverageArtist = new Text("Most Average Artist: ");
    private double maxDistance = Double.MIN_VALUE;
    private double minDistance = Double.MAX_VALUE;
    private String uniqueArtistId = "";
    private String averageArtistId = "";

    @Override
    public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
            throws IOException, InterruptedException {
        double max = Double.MIN_VALUE;
        double min = Double.MAX_VALUE;

        // Iterate over all distances for a given artist
        for (DoubleWritable val : values) {
            double distance = val.get();
            if (distance > max) {
                max = distance;
            }
            if (distance < min) {
                min = distance;
            }
        }

        // Check if the current artist's max distance is the largest overall
        if (max > maxDistance) {
            maxDistance = max;
            uniqueArtistId = key.toString();
        }

        // Check if the current artist's min distance is the smallest overall
        if (min < minDistance) {
            minDistance = min;
            averageArtistId = key.toString();
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        // Emit the final most unique and most average artist IDs with their distances
        context.write(mostUniqueArtist, new Text(uniqueArtistId + " with distance: " + maxDistance));
        context.write(mostAverageArtist, new Text(averageArtistId + " with distance: " + minDistance));
    }
}