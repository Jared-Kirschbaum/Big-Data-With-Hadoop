package csx55.hadoop.q8;
import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

public class SegmentDataMapper extends Mapper<LongWritable, Text, Text, Text> {

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] parts = value.toString().split("\\|");

        String authorId = parts[33];
        
        
// Prepend author_id to the key so that all data from the same author goes to the same reducer
    context.write(new Text(authorId + "|start_time"), new Text(cleanFeature(parts[17])));
    context.write(new Text(authorId + "|pitch"), new Text(cleanFeature(parts[19])));
    context.write(new Text(authorId + "|timbre"), new Text(cleanFeature(parts[20])));
    context.write(new Text(authorId + "|max_loudness"), new Text(cleanFeature(parts[21])));
    context.write(new Text(authorId + "|max_loudness_time"), new Text(cleanFeature(parts[22])));
    context.write(new Text(authorId + "|start_loudness"), new Text(cleanFeature(parts[23])));
}

    private String cleanFeature(String featureData) {
        // Clean the feature data by removing brackets and splitting by space
        return featureData.replace("[", "").replace("]", "").trim();
    }
}
