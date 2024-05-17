package csx55.hadoop.q10;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class CosineSimilarityMapper extends Mapper<Object, Text, Text, Text> {
    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        // Assuming value format: "term^songId    tfidfScore"
        String[] parts = value.toString().split("\\t");
        if (parts.length == 2) {
            String[] termSong = parts[0].split("\\^");
            if (termSong.length == 2) {
                String term = termSong[0];
                String songId = termSong[1];
                Double tfidfScore = Double.parseDouble(parts[1]);  // Convert score to Double
                context.write(new Text(songId), new Text(term + "^" + tfidfScore));  // Emit songId^term and tfidfScore
            }
        }
    }
}
