package csx55.hadoop.q5;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;


public class FreqReducer extends Reducer<IntWritable, FloatWritable, Text, FloatWritable> {
    private List<Float> durations = new ArrayList<>();

    public void reduce(IntWritable key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {
        for (FloatWritable val : values) {
            durations.add(val.get());
        }
        Collections.sort(durations);

        int size = durations.size();
        float median = (size % 2 == 1) ? durations.get(size/2) : (durations.get(size/2 - 1) + durations.get(size/2)) / 2.0f;
        context.write(new Text("Med"), new FloatWritable(median));
        context.write(new Text("Min"), new FloatWritable(durations.get(0)));
        context.write(new Text("Max"), new FloatWritable(durations.get(size - 1)));
    }
}