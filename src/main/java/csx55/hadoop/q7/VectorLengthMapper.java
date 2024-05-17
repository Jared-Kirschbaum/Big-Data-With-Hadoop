package csx55.hadoop.q7;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class VectorLengthMapper extends Mapper<Object, Text, Text, IntWritable> {
    private Text dataKey = new Text();
    private IntWritable vectorLength = new IntWritable();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] parts = value.toString().split("\\t");
        if (parts.length >= 2) {
            String[] vectorElements = parts[1].split(" ");
            dataKey.set(parts[0]); // set your key
            vectorLength.set(vectorElements.length); // set the length of the vector
            context.write(dataKey, vectorLength);
        }
    }
}
