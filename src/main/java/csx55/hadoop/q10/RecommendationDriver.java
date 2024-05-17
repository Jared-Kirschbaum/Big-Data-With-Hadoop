package csx55.hadoop.q10;

import csx55.hadoop.joiner.JoinReducer;
import csx55.hadoop.joiner.MetadataFileMapper;
import csx55.hadoop.joiner.AnalysisFileMapper;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.net.URI;

import csx55.hadoop.common.DescendingIntWritableComparator;
import csx55.hadoop.common.DescendingDoubleWritableComparator;


public class RecommendationDriver {
    public static boolean main(String[] args) throws Exception {


        // Job 1: Join datasets
        Configuration joinConf = new Configuration();
        Job joinJob = Job.getInstance(joinConf, "Join Analysis and Metadata");
        joinJob.setJarByClass(RecommendationDriver.class);
        joinJob.setReducerClass(JoinReducer.class);
        joinJob.setOutputKeyClass(Text.class);
        joinJob.setOutputValueClass(Text.class);

        MultipleInputs.addInputPath(joinJob, new Path(args[0]), TextInputFormat.class, AnalysisFileMapper.class);
        MultipleInputs.addInputPath(joinJob, new Path(args[1]), TextInputFormat.class, MetadataFileMapper.class);
        FileOutputFormat.setOutputPath(joinJob, new Path(args[2]));

        if (!joinJob.waitForCompletion(true)) {
            System.err.println("Join job failed, exiting...");
            System.exit(1);
        }

        
        // Job 2: clean and set values for terms and songID's
         Configuration avgConf = new Configuration();
         Job avgJob = Job.getInstance(avgConf, "Calculate Average Loudness");
         avgJob.setJarByClass(RecommendationDriver.class);
         avgJob.setMapperClass(TermColumnMapper.class);
         avgJob.setReducerClass(TermColumnReducer.class);
 
         avgJob.setMapOutputKeyClass(Text.class);
         avgJob.setMapOutputValueClass(Text.class);
         avgJob.setOutputKeyClass(Text.class);
         avgJob.setOutputValueClass(Text.class);
 
         FileInputFormat.addInputPath(avgJob, new Path(args[2]));
         FileOutputFormat.setOutputPath(avgJob, new Path(args[3]));
 
         if (!avgJob.waitForCompletion(true)) {
             System.err.println("Average loudness calculation job failed, exiting...");
             System.exit(1);
         }


         // Job 3: Obtain Term frequency
         Configuration freqConf = new Configuration();
         Job freqJob = Job.getInstance(freqConf, "Calculate Average Loudness");
         freqJob.setJarByClass(RecommendationDriver.class);
         freqJob.setMapperClass(TermFrequencyMapper.class);
         freqJob.setReducerClass(TermFrequencyReducer.class);
 
         freqJob.setMapOutputKeyClass(Text.class);
         freqJob.setMapOutputValueClass(IntWritable.class);
         freqJob.setOutputKeyClass(Text.class);
         freqJob.setOutputValueClass(IntWritable.class);
 
         FileInputFormat.addInputPath(freqJob, new Path(args[3]));
         FileOutputFormat.setOutputPath(freqJob, new Path(args[4]));
 
         if (!freqJob.waitForCompletion(true)) {
             System.err.println("Average loudness calculation job failed, exiting...");
             System.exit(1);
         }

         // Obtain IDF scores
         Configuration invConf = new Configuration();
         Job invJob = Job.getInstance(invConf, "Calculate Average Loudness");
         invJob.setJarByClass(RecommendationDriver.class);
         invJob.setMapperClass(IDFCalculationMapper.class);
         invJob.setReducerClass(IDFCalculationReducer.class);
 
         invJob.setMapOutputKeyClass(Text.class);
         invJob.setMapOutputValueClass(Text.class);
         invJob.setOutputKeyClass(Text.class);
         invJob.setOutputValueClass(DoubleWritable.class);
 
         FileInputFormat.addInputPath(invJob, new Path(args[3]));
         FileOutputFormat.setOutputPath(invJob, new Path(args[8]));
 
         if (!invJob.waitForCompletion(true)) {
             System.err.println("Average loudness calculation job failed, exiting...");
             System.exit(1);
         }

        // Job 3: Obtain TFIDF matrix
        Configuration tfidfConf = new Configuration();
        Job tfidfJob = Job.getInstance(tfidfConf, "Calculate Average Loudness");
        tfidfJob.setJarByClass(RecommendationDriver.class);
        tfidfJob.setMapperClass(TFIDFMapper.class);
        tfidfJob.setReducerClass(TFIDFReducer.class);

        tfidfJob.setMapOutputKeyClass(Text.class);
        tfidfJob.setMapOutputValueClass(Text.class);
        tfidfJob.setOutputKeyClass(Text.class);
        tfidfJob.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(tfidfJob, new Path(args[4]));
        FileOutputFormat.setOutputPath(tfidfJob, new Path(args[5]));
 
        if (!tfidfJob.waitForCompletion(true)) {
             System.err.println("Average loudness calculation job failed, exiting...");
             System.exit(1);
        }
        
        // Pass the query terms to the mappers and reducers

        Configuration qConf = new Configuration();

        qConf.set("queryTerms", args[7]);

        Job qJob = Job.getInstance(qConf, "Song Recommendation System");

        qJob.addCacheFile(new URI("hdfs://raleigh.cs.colostate.edu:31640/ms/q10/IDF/part-r-00000#idfScores"));

        qJob.setJarByClass(RecommendationDriver.class);
        qJob.setMapperClass(CosineSimilarityMapper.class);
        qJob.setReducerClass(CosineSimilarityReducer.class);

        qJob.setMapOutputKeyClass(Text.class);
        qJob.setMapOutputValueClass(Text.class);  // Ensure mapper output value type is DoubleWritable
        qJob.setOutputKeyClass(Text.class);
        qJob.setOutputValueClass(DoubleWritable.class); 

        qJob.setSortComparatorClass(DescendingDoubleWritableComparator.class);

        FileInputFormat.addInputPath(qJob, new Path(args[5]));
        FileOutputFormat.setOutputPath(qJob, new Path(args[6]));

        if (!qJob.waitForCompletion(true)) {
            System.err.println("Query Failed");
            System.exit(1);
       }


       Configuration sortConf = new Configuration();

       Job sortJob = Job.getInstance(sortConf, "Song Recommendation System");

      
       sortJob.setJarByClass(RecommendationDriver.class);
       sortJob.setMapperClass(SortValuesMapper.class);
       sortJob.setReducerClass(SortValuesReducer.class);

       sortJob.setMapOutputKeyClass(DoubleWritable.class);
       sortJob.setMapOutputValueClass(Text.class);
       sortJob.setOutputKeyClass(Text.class);
       sortJob.setOutputValueClass(DoubleWritable.class); 

       sortJob.setSortComparatorClass(DescendingDoubleWritableComparator.class);

       FileInputFormat.addInputPath(sortJob, new Path(args[6]));
       FileOutputFormat.setOutputPath(sortJob, new Path(args[9]));

       if (!sortJob.waitForCompletion(true)) {
           System.err.println("Query Failed");
           System.exit(1);
      }

        return true;
    }
}
