package csx55.hadoop.q5;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class MatchReducer extends Reducer<FloatWritable, Text, Text, Text> {
    public void reduce(FloatWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        List<String> songs = new ArrayList<>();
        boolean isStat = false;

        for (Text val : values) {
            if (val.toString().equals("Max") || val.toString().equals("Min") || val.toString().equals("Med")) {
                isStat = true;
            } else {
                songs.add(val.toString());
            }
        }

        if (isStat) {
            for (String song : songs) {
                context.write(new Text(song), new Text(key.toString()));
            }
        }
    }
}

