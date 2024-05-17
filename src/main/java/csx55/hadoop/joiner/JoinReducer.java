package csx55.hadoop.joiner;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
//import org.apache.log4j.Logger;

public class JoinReducer extends Reducer<Text, Text, Text, Text> {

    //private static final Logger LOG = Logger.getLogger(JoinReducer.class);

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        // Initialize lists to hold records from analysis and metadata files
        List<String> analysisRecords = new ArrayList<>();
        List<String> metadataRecords = new ArrayList<>();

        for (Text value : values) {
            String[] parts = value.toString().split("\\|");

            //LOG.info(parts.length);

            
            if (parts.length == 32) {
                analysisRecords.add(value.toString());
            } else if (parts.length == 14) {
                metadataRecords.add(value.toString());
            }
        }

        for (String analysisRecord : analysisRecords) {
            for (String metadataRecord : metadataRecords) {
                context.write(new Text(analysisRecord), new Text(metadataRecord));
            }
        }
    }
}

