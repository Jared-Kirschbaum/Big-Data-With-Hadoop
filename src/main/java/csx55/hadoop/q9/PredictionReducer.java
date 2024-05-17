package csx55.hadoop.q9;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.Iterator;

public class PredictionReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) 
            throws IOException, InterruptedException {
        double sum = 0.0;
        int count = 0;

        // if there were multiple predictions per feature
        Iterator<Text> iter = values.iterator();
        while (iter.hasNext()) {
            sum += Double.parseDouble(iter.next().toString());
            count++;
        }
        
        double average = (count > 0) ? (sum / count) : 0.0;
        context.write(key, new Text(String.valueOf(average)));
    }
}
