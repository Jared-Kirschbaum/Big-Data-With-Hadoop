package csx55.hadoop.q10;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.text.DecimalFormat;
import java.lang.Math;

public class TFIDFReducer extends Reducer<Text, Text, Text, DoubleWritable> {
    private static final DecimalFormat decimalFormat = new DecimalFormat("#.###");
    private static final int numSongs = 10000;  // Assuming total number of songs is 10,000

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) 
            throws IOException, InterruptedException {
        int termFrequency = 0;
        for (Text value : values) {
            termFrequency += Integer.parseInt(value.toString());  // Convert Text to integer
        }

        double idf = Math.log((double) numSongs / (1 + termFrequency));
        double tfidf = termFrequency * idf;

        context.write(key, new DoubleWritable(Double.parseDouble(decimalFormat.format(tfidf))));
    }
}
