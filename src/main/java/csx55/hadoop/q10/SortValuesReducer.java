package csx55.hadoop.q10;


import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

public class SortValuesReducer extends Reducer<DoubleWritable, Text, Text, DoubleWritable> {
    @Override
    public void reduce(DoubleWritable key, Iterable<Text> values, Context context) 
            throws IOException, InterruptedException {
        for (Text value : values) {
            context.write(value, key);  // Emit original value and score
        }
    }
}
