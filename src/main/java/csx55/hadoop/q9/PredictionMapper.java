package csx55.hadoop.q9;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;


import org.apache.log4j.Logger;


public class PredictionMapper extends Mapper<Object, Text, Text, Text> {


   // private static final Logger LOG = Logger.getLogger(PredictionMapper.class);

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] parts = value.toString().split("\\t");
        
        String featureName = parts[0];
        String myVals = parts[1];
        String[] alphaBeta = myVals.split(",");
        String betaOne = alphaBeta[1].split("=")[1];
        String alphaOne = alphaBeta[0].split("=")[1];


        // Using a fixed hotness of 1.1 to predict each feature
        double hotness = 1.1;
        double alpha = Double.parseDouble(alphaOne);
        double beta = Double.parseDouble(betaOne);
        double predictedValue = alpha + beta * hotness;

        context.write(new Text(featureName), new Text(String.valueOf(predictedValue)));
    }
}
