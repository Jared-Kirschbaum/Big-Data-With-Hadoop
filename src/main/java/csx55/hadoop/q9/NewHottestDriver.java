package csx55.hadoop.q9;

import csx55.hadoop.joiner.JoinReducer;
import csx55.hadoop.joiner.MetadataFileMapper;
import csx55.hadoop.joiner.AnalysisFileMapper;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import csx55.hadoop.common.DescendingIntWritableComparator;

public class NewHottestDriver {
    public static boolean main(String[] args) throws Exception {


        // Job 1: Join datasets
        Configuration joinConf = new Configuration();
        Job joinJob = Job.getInstance(joinConf, "Join Analysis and Metadata");
        joinJob.setJarByClass(NewHottestDriver.class);
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

        // Job 2: Calculate average loudness per artist
        Configuration avgConf = new Configuration();
        Job avgJob = Job.getInstance(avgConf, "Calculate Average Loudness");
        avgJob.setJarByClass(NewHottestDriver.class);
        avgJob.setMapperClass(ColumnMapper.class);
        avgJob.setReducerClass(ColumnReducer.class);

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


        // Job 3: Calculate co efficents of LR.
        Configuration tuneConf = new Configuration();
        Job tuneJob = Job.getInstance(tuneConf, "Calculate Average Loudness");
        tuneJob.setJarByClass(NewHottestDriver.class);
        tuneJob.setMapperClass(RegressionMapper.class);
        tuneJob.setReducerClass(RegressionReducer.class);

        tuneJob.setMapOutputKeyClass(Text.class);
        tuneJob.setMapOutputValueClass(Text.class);
        tuneJob.setOutputKeyClass(Text.class);
        tuneJob.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(tuneJob, new Path(args[3]));
        FileOutputFormat.setOutputPath(tuneJob, new Path(args[4]));

        if (!tuneJob.waitForCompletion(true)) {
            System.err.println("LR Job Failed.");
            System.exit(1);
        }

        

        // Job 4: Predict values for hotness at 1.1
        Configuration predConf = new Configuration();
        Job predJob = Job.getInstance(predConf, "Calculate Average Loudness");
        predJob.setJarByClass(NewHottestDriver.class);
        predJob.setMapperClass(PredictionMapper.class);
        predJob.setReducerClass(PredictionReducer.class);

        predJob.setMapOutputKeyClass(Text.class);
        predJob.setMapOutputValueClass(Text.class);
        predJob.setOutputKeyClass(Text.class);
        predJob.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(predJob, new Path(args[4]));
        FileOutputFormat.setOutputPath(predJob, new Path(args[5]));

        if (!predJob.waitForCompletion(true)) {
            System.err.println("Pred Job failed");
            System.exit(1);
        }

        Configuration termConf = new Configuration();
        Job termJob = Job.getInstance(termConf, "Term Count");
        termJob.setJarByClass(NewHottestDriver.class);
        termJob.setMapperClass(TermsMapper.class);
        termJob.setReducerClass(TermsReducer.class);

        termJob.setOutputKeyClass(Text.class);
        termJob.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(termJob, new Path(args[3]));
        FileOutputFormat.setOutputPath(termJob, new Path(args[6]));

        if (!termJob.waitForCompletion(true)) {
            System.err.println("termJob failed");
            System.exit(1);
        }

        Configuration maxConf = new Configuration();
        Job maxJob = Job.getInstance(maxConf, "max term count");
        maxJob.setJarByClass(NewHottestDriver.class);
        maxJob.setMapperClass(GlobalTopTenMapper.class);
        maxJob.setReducerClass(GlobalTopTermsReducer.class);

        maxJob.setMapOutputKeyClass(IntWritable.class);
        maxJob.setMapOutputValueClass(Text.class);
        maxJob.setOutputKeyClass(Text.class);
        maxJob.setOutputValueClass(IntWritable.class);

        maxJob.setSortComparatorClass(DescendingIntWritableComparator.class);

        FileInputFormat.addInputPath(maxJob, new Path(args[6]));
        FileOutputFormat.setOutputPath(maxJob, new Path(args[7]));

        if (!maxJob.waitForCompletion(true)) {
            System.err.println("maxJob failed");
            System.exit(1);
        }
        return true;
    }
}
