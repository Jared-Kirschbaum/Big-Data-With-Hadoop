package csx55.hadoop.q9;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

public class RegressionReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) 
            throws IOException, InterruptedException {
        // Skip processing for the "terms" column
        if (key.toString().equals("terms")) {
            return;  // Exit the reduce method early for "terms"
        }

        // Perform regression calculation for other features
        double sumX = 0, sumY = 0, sumXY = 0, sumXX = 0, n = 0;
        for (Text val : values) {
            String[] parts = val.toString().split(",");
            double x = parseDouble(parts[1]); // hotness
            double y = parseDouble(parts[0]); // feature value
            sumX += x;
            sumY += y;
            sumXY += x * y;
            sumXX += x * x;
            n++;
        }

        if (n < 2 || (sumXX - (sumX * sumX / n)) == 0) {
            context.write(key, new Text("alpha=NaN,beta=NaN"));
        } else {
            double beta = (n * sumXY - sumX * sumY) / (n * sumXX - sumX * sumX);
            double alpha = (sumY - beta * sumX) / n;
            context.write(key, new Text("alpha=" + alpha + ",beta=" + beta));
        }
    }

    // Helper method to parse double or return 0.0 if NaN
    private double parseDouble(String str) {
        try {
            return Double.parseDouble(str);
        } catch (NumberFormatException e) {
            return 0.0;
        }
    }
}
