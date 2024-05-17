package csx55.hadoop.q5;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.FloatWritable;

import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class StatMapper extends Mapper<Object, Text, FloatWritable, Text> {
    private FloatWritable statValue = new FloatWritable();
    private Text statType = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] parts = value.toString().split("\\s+");
        if (parts.length == 2) {
            try {
                float val = Float.parseFloat(parts[1]);
                statValue.set(val);
                statType.set(parts[0]);
                context.write(statValue, statType);
            } catch (NumberFormatException e) {
                // Handle parse error or ignore the record
            }
        }
    }
}