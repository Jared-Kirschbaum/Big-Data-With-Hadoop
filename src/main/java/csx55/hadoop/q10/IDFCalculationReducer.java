package csx55.hadoop.q10;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;



public class IDFCalculationReducer extends Reducer<Text, Text, Text, DoubleWritable> {
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Set<String> uniqueDocuments = new HashSet<>();
        for (Text value : values) {
            uniqueDocuments.add(value.toString());
        }
        int numDocuments = uniqueDocuments.size();
        double idfScore = Math.log((double) getTotalNumberOfDocuments(context) / (1 + numDocuments));
        context.write(key, new DoubleWritable(idfScore));
    }

    private int getTotalNumberOfDocuments(Context context) {
        // This should be set in your job configuration
        return context.getConfiguration().getInt("totalDocuments", 1);
    }
}
