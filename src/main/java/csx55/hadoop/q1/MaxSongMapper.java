package csx55.hadoop.q1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MaxSongMapper extends Mapper<Object, Text, IntWritable, Text> {
    private IntWritable songCount = new IntWritable();
    private Text artistId = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] parts = value.toString().split("\\t");
        if (parts.length == 2) {
            int count = Integer.parseInt(parts[1]);
            songCount.set(count);
            artistId.set(parts[0]);

            // swapping so songCount is the key and artistId is the value to handle distributed reducer
            context.write(songCount, artistId);
        }
    }
}
