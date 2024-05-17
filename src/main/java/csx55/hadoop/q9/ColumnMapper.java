package csx55.hadoop.q9;


import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class ColumnMapper extends Mapper<Object, Text, Text, Text> {
    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] parts = value.toString().split("\\|");

        double hotness = parseDouble(parts[1]); // Safely parse hotness, replacing "nan" with "0.0"
        int[] featureIndices = {3, 4, 5, 6, 7, 9, 10, 13, 14, 12, 41}; // Indices of the features
        String[] featureNames = {"dance", "duration", "end_of_fade_in", "energy", "key",
                                 "loudness", "mode", "tempo", "time_sig", "start_of_fade_out", "terms"};

        for (int i = 0; i < featureIndices.length; i++) {
            String rawFeature = cleanText(parts[featureIndices[i]]);
            if (featureNames[i].equals("terms")) {
                // If the feature is "terms", just write it out without parsing to double
                context.write(new Text(featureNames[i]), new Text(hotness + "," + rawFeature));
            } else {
                if (rawFeature.isEmpty()) {
                    rawFeature = "0.0"; // Ensure no empty strings are processed further
                }
                String[] vectorComponents = rawFeature.split("\\s+"); // Split cleaned vector into components
                for (String component : vectorComponents) {
                    double featureValue = parseDouble(component); // Safely parse each cleaned component
                    String outputValue = hotness + "," + featureValue;
                    context.write(new Text(featureNames[i]), new Text(outputValue));
                }
            }
        }
    }

    private double parseDouble(String str) {
        if (str.trim().equalsIgnoreCase("nan") || str.trim().isEmpty()) {
            return 0.0; // Replace "nan" or empty strings with 0.0
        }
        return Double.parseDouble(str); // Parse normally if not "nan" or empty
    }

    private String cleanText(String text) {
        text = text.replaceAll("\\[|\\]", ""); // Remove all occurrences of [ and ]
        if (text.trim().isEmpty()) { // Check if the text is empty after cleaning
            return "0.0"; // Return "0.0" if text is empty
        }
        return text;
    }
}

