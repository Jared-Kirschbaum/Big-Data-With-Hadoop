package csx55.hadoop.q7;

import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

public class SegmentDataReducer extends Reducer<Text, Text, Text, Text> {
    
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
           for(Text val : values) {
            context.write(key, val);
        }
    }
}