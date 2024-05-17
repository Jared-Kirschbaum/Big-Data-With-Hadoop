package csx55.hadoop.q10;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class QueryVectorMapper extends Mapper<Object, Text, Text, DoubleWritable> {
    private Map<String, Double> queryVector;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        String queryTerms = context.getConfiguration().get("queryTerms");
        queryVector = buildQueryVector(queryTerms);
    }

    private static Map<String, Double> buildQueryVector(String query) {
        Map<String, Double> vector = new HashMap<>();
        String[] terms = query.split("\\s+");
        for (String term : terms) {
            // For simplicity, using 1.0 as the score; this can be changed to actual TF-IDF scores if available
            vector.put(term, 1.0); 
        }
        return vector;
    }

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        // Emit each term in the query with its score (for now, using 1.0 as placeholder)
        for (Map.Entry<String, Double> entry : queryVector.entrySet()) {
            context.write(new Text(entry.getKey()), new DoubleWritable(entry.getValue()));
        }
    }
}

