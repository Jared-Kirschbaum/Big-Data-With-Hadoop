package csx55.hadoop.q10;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.io.BufferedReader;
import java.io.FileReader;
import java.net.URI;
import org.apache.log4j.Logger;



public class CosineSimilarityReducer extends Reducer<Text, Text, Text, DoubleWritable> {
    private Map<String, Double> idfScores = new HashMap<>();
    private Map<String, Double> queryVector = new HashMap<>();
    private static final Logger logger = Logger.getLogger(CosineSimilarityReducer.class);


    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        // Load the IDF scores from the Distributed Cache
        URI[] cacheFiles = context.getCacheFiles();
        if (cacheFiles != null && cacheFiles.length > 0) {
            BufferedReader bufferedReader = new BufferedReader(new FileReader("idfScores"));
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                String[] parts = line.split("\\t");
                if (parts.length == 2) {
                    idfScores.put(parts[0], Double.parseDouble(parts[1]));
                }
            }
            bufferedReader.close();
        }

        // passed via configuration and compute their TF-IDF using loaded IDF scores
        String[] queryTerms = context.getConfiguration().get("queryTerms").split("\\s+");
        for (String term : queryTerms) {
            double tf = 1.0 / queryTerms.length; // Simple term frequency (TF) for example
            double idf = idfScores.getOrDefault(term, 0.0); // Get IDF from loaded scores
            queryVector.put(term, tf * idf); // Compute TF-IDF and store in query vector
        }
    }

public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Map<String, Double> documentVector = new HashMap<>();
        for (Text value : values) {
            String[] parts = value.toString().split("\\^");
            if (parts.length == 2) {
                String term = parts[0];
                double tfidfScore = Double.parseDouble(parts[1]);
                documentVector.put(term, tfidfScore);
            }
        }

        double dotProduct = 0.0;
        double normQuery = 0.0;
        double normDocument = 0.0;

        // Compute dot product and norms
        for (Map.Entry<String, Double> queryEntry : queryVector.entrySet()) {
            String term = queryEntry.getKey();
            Double queryTfidf = queryEntry.getValue();
            normQuery += Math.pow(queryTfidf, 2);

            if (documentVector.containsKey(term)) {
                Double docTfidf = documentVector.get(term);
                dotProduct += queryTfidf * docTfidf;
                normDocument += Math.pow(docTfidf, 2);  // This part calculates the document vector norm
            }
        }

        // Add the missing vector norms for non-overlapping terms in document
        for (Map.Entry<String, Double> docEntry : documentVector.entrySet()) {
            if (!queryVector.containsKey(docEntry.getKey())) {
                normDocument += Math.pow(docEntry.getValue(), 2);
            }
        }

        double cosineSimilarity = 0;
        if (normQuery != 0 && normDocument != 0) {
            cosineSimilarity = dotProduct / (Math.sqrt(normQuery) * Math.sqrt(normDocument));
        }

        context.write(key, new DoubleWritable(cosineSimilarity));
    }
}
