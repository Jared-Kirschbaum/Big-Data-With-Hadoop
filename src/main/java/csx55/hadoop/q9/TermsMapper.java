package csx55.hadoop.q9;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class TermsMapper extends Mapper<Object, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] parts = value.toString().split("\\t");
        if (parts[0].equals("terms")) {
            String[] terms = parts[1].substring(parts[1].indexOf(",") + 1).replace("'", "").split("\\s+");
            for (String term : terms) {
                word.set(term);
                context.write(word, one);
            }
        }
    }
}

