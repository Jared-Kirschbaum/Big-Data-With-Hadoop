package csx55.hadoop.q9;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class RegressionMapper extends Mapper<Object, Text, Text, Text> {
    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] parts = value.toString().split("\\t");
        String columnName = parts[0]; // Column name like "dance"
        String[] values = parts[1].split(","); // Split the value and hotness
       // Try to parse double values, if fail, skip the entry
       try {
        double featureValue = Double.parseDouble(values[0]);
        double hotness = Double.parseDouble(values[1]);
        context.write(new Text(columnName), new Text(featureValue + "," + hotness));
        } catch (NumberFormatException e) {

        }
    }   
}