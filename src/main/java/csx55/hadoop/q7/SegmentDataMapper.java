package csx55.hadoop.q7;

import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

public class SegmentDataMapper extends Mapper<LongWritable, Text, Text, Text> {

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] parts = value.toString().split("\\|");
        
        // Emit each feature as a Text array
        context.write(new Text("start_time"), new Text(cleanFeature(parts[17])));
        context.write(new Text("pitch"), new Text(cleanFeature(parts[19])));
        context.write(new Text("timbre"), new Text(cleanFeature(parts[20])));
        context.write(new Text("max_loudness"), new Text(cleanFeature(parts[21])));
        context.write(new Text("max_loudness_time"), new Text(cleanFeature(parts[22])));
        context.write(new Text("start_loudness"), new Text(cleanFeature(parts[23])));
    }

    private String cleanFeature(String featureData) {
        // Clean the feature data by removing brackets and splitting by space
        return featureData.replace("[", "").replace("]", "").trim();
    }
}
