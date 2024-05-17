package csx55.hadoop.q10;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class IDFCalculationMapper extends Mapper<Object, Text, Text, Text> {
    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        Set<String> uniqueTerms = new HashSet<>();
        // format is "songId   terms..."
        String[] parts = value.toString().split("\\t");
        if (parts.length > 1) {
            String[] terms = parts[1].split("\\s+");
            for (String term : terms) {
                if (!uniqueTerms.contains(term)) {
                    context.write(new Text(term), new Text(parts[0])); // Emit term and songId
                    uniqueTerms.add(term);
                }
            }
        }
    }
}
