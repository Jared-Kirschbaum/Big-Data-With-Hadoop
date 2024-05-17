package csx55.hadoop.q10;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;


public class TermFrequencyMapper extends Mapper<Object, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private Text termSong = new Text();

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] parts = value.toString().split("\\t");
        String songId = parts[0];
        String[] terms = parts[1].split("\\s+");

        for (String term : terms) {
            termSong.set(term + "^" + songId);
            context.write(termSong, one);
        }
    }
}
