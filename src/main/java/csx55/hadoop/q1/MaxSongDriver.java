package csx55.hadoop.q1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import csx55.hadoop.common.DescendingIntWritableComparator;

public class MaxSongDriver {
    public static boolean main(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: MaxSongDriver <input path> <intermediate output path> <final output path>");
            System.exit(-1);
        }

        // Setup and execute the Song Count job
        Configuration countConf = new Configuration();
        Job countJob = Job.getInstance(countConf, "Count Songs Per Artist");
        countJob.setJarByClass(MaxSongDriver.class);
        countJob.setMapperClass(SongCountMapper.class);
        countJob.setReducerClass(SongCountReducer.class);

        countJob.setMapOutputKeyClass(Text.class);
        countJob.setMapOutputValueClass(IntWritable.class);
        countJob.setOutputKeyClass(Text.class);
        countJob.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(countJob, new Path(args[0]));  // Input path
        FileOutputFormat.setOutputPath(countJob, new Path(args[1]));  // Intermediate output path

        boolean countCompleted = countJob.waitForCompletion(true);
        if (!countCompleted) {
            System.err.println("Count job failed, exiting...");
            System.exit(1);
        }

        // Setup and execute the Max Song job
        Configuration maxConf = new Configuration();
        Job maxJob = Job.getInstance(maxConf, "Find Max Songs Artist");
        maxJob.setJarByClass(MaxSongDriver.class);
        maxJob.setMapperClass(MaxSongMapper.class);
        maxJob.setReducerClass(MaxSongReducer.class);

        maxJob.setOutputKeyClass(IntWritable.class);
        maxJob.setOutputValueClass(Text.class);

        maxJob.setSortComparatorClass(DescendingIntWritableComparator.class);

        

        FileInputFormat.addInputPath(maxJob, new Path(args[1]));  // Input from count job
        FileOutputFormat.setOutputPath(maxJob, new Path(args[2]));  // Final output path

        boolean maxJobCompleted = maxJob.waitForCompletion(true);
        if (!maxJobCompleted) {
            System.err.println("Count job failed.");
            return false;
        }
        return true;
    }
}