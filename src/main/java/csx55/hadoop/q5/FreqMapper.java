package csx55.hadoop.q5;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class FreqMapper  extends Mapper<Object, Text, IntWritable, FloatWritable> {

    private static final IntWritable one = new IntWritable(1);
    private FloatWritable durationValue = new FloatWritable();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] parts = value.toString().split("\\|");
        if (parts.length >= 2) {
            float duration = Float.parseFloat(parts[4]);
            durationValue.set(duration);
            context.write(one, durationValue);
        }
    }
}