package csx55.hadoop.joiner;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class AnalysisFileMapper extends Mapper<LongWritable, Text, Text, Text> {
    private Text songId = new Text();
    private Text record = new Text();

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] parts = value.toString().split("\\|");
        if (parts.length == 32) { // Checking for analysis file structure
            songId.set(parts[0]); // song_id is the first column for analysis
            record.set(value);
            context.write(songId, record);
        }
    }
}

