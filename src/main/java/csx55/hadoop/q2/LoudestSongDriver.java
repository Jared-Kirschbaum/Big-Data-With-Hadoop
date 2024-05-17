package csx55.hadoop.q2;

import csx55.hadoop.joiner.JoinReducer;
import csx55.hadoop.joiner.MetadataFileMapper;
import csx55.hadoop.joiner.AnalysisFileMapper;
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

public class LoudestSongDriver {
    public static boolean main(String[] args) throws Exception {
        if (args.length != 5) {
            System.err.println("Usage: LoudestSongDriver <input path analysis> <input path metadata> <output path for join> <intermediate output path> <final output path>");
            System.exit(-1);
        }

        // Job 1: Join datasets
        Configuration joinConf = new Configuration();
        Job joinJob = Job.getInstance(joinConf, "Join Analysis and Metadata");
        joinJob.setJarByClass(LoudestSongDriver.class);
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
        avgJob.setJarByClass(LoudestSongDriver.class);
        avgJob.setMapperClass(LoudestSongMapper.class);
        avgJob.setReducerClass(LoudestSongReducer.class);

        avgJob.setMapOutputKeyClass(Text.class);
        avgJob.setMapOutputValueClass(FloatWritable.class);
        avgJob.setOutputKeyClass(Text.class);
        avgJob.setOutputValueClass(FloatWritable.class);

        FileInputFormat.addInputPath(avgJob, new Path(args[2]));
        FileOutputFormat.setOutputPath(avgJob, new Path(args[3]));

        if (!avgJob.waitForCompletion(true)) {
            System.err.println("Average loudness calculation job failed, exiting...");
            System.exit(1);
        }

        // Job 3: Find the artist with the maximum average loudness
        Configuration maxConf = new Configuration();
        Job maxJob = Job.getInstance(maxConf, "Find Max Average Loudness");
        maxJob.setJarByClass(LoudestSongDriver.class);
        maxJob.setMapperClass(MaxLoudnessMapper.class);
        maxJob.setReducerClass(MaxLoudnessReducer.class);

        maxJob.setMapOutputKeyClass(FloatWritable.class);
        maxJob.setMapOutputValueClass(Text.class);
        maxJob.setOutputKeyClass(FloatWritable.class);
        maxJob.setOutputValueClass(Text.class);

        maxJob.setSortComparatorClass(DescendingIntWritableComparator.class);

        FileInputFormat.addInputPath(maxJob, new Path(args[3])); // Use intermediate output as input
        FileOutputFormat.setOutputPath(maxJob, new Path(args[4])); // Final output
        
        boolean maxJobCompleted = maxJob.waitForCompletion(true);
        if (!maxJobCompleted) {
            System.err.println("Count job failed.");
            return false;
        }

        return true;
    }
}
