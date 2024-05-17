package csx55.hadoop.joiner;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MetadataFileMapper extends Mapper<LongWritable, Text, Text, Text> {
    private Text songId = new Text();
    private Text record = new Text();

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] parts = value.toString().split("\\|");
        if (parts.length == 14) { // Checking for Metadata file structure
            songId.set(parts[7]); // song_id is the first column for Metadata
            record.set(value);
            context.write(songId, record);
        }
    }
}

