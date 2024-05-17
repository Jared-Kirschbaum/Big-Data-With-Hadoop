package csx55.hadoop.q10;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class SortValuesMapper extends Mapper<Object, Text, DoubleWritable, Text> {
    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] parts = value.toString().split("\\t");
        if (parts.length == 2) {
            double score = Double.parseDouble(parts[1]);  // Assuming the score is in the second part
            context.write(new DoubleWritable(score), new Text(parts[0]));  // Emit score as key
        }
    }
}