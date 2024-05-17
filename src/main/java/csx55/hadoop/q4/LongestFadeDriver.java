package csx55.hadoop.q4;

import csx55.hadoop.joiner.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import csx55.hadoop.common.*;

public class LongestFadeDriver {
    public static boolean main(String[] args) throws Exception {
        if (args.length != 5) {
            System.err.println("Usage: HottestSongDriver <input path analysis> <input path metadata> <output path for join> <intermediate path> <final output path>");
            System.exit(-1);
        }

        // Job 1: Join datasets
        Configuration joinConf = new Configuration();
        Job joinJob = Job.getInstance(joinConf, "Join Analysis and Metadata");
        joinJob.setJarByClass(LongestFadeDriver.class);
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

    
        Configuration sumConf = new Configuration();
        Job sumJob = Job.getInstance(sumConf, "Hottest Song Finder");
        sumJob.setJarByClass(LongestFadeDriver.class);
        sumJob.setMapperClass(LongestFadeMapper.class);
        sumJob.setReducerClass(LongestFadeReducer.class);
    
        sumJob.setMapOutputKeyClass(Text.class);
        sumJob.setMapOutputValueClass(FloatWritable.class);
        sumJob.setOutputKeyClass(Text.class);
        sumJob.setOutputValueClass(FloatWritable.class);
    
        FileInputFormat.addInputPath(sumJob, new Path(args[2]));
        FileOutputFormat.setOutputPath(sumJob, new Path(args[3]));

        if (!sumJob.waitForCompletion(true)) {
            System.err.println("Join sumJob failed, exiting...");
            System.exit(1);
        }


        Configuration maxConf = new Configuration();
        Job maxJob = Job.getInstance(maxConf, "Max Hottest Song Finder");
        maxJob.setJarByClass(LongestFadeDriver.class);
        maxJob.setMapperClass(MaxFadeMapper.class);
        maxJob.setReducerClass(MaxFadeReducer.class);
    
        maxJob.setMapOutputKeyClass(FloatWritable.class);
        maxJob.setMapOutputValueClass(Text.class);
        maxJob.setOutputKeyClass(Text.class);
        maxJob.setOutputValueClass(FloatWritable.class);

        maxJob.setSortComparatorClass(DescendingIntWritableComparator.class);
    
        FileInputFormat.addInputPath(maxJob, new Path(args[3]));
        FileOutputFormat.setOutputPath(maxJob, new Path(args[4]));
    
        return maxJob.waitForCompletion(true);
    }
    
}