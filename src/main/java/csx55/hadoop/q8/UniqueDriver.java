package csx55.hadoop.q8;

import csx55.hadoop.joiner.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import java.net.URI;

public class UniqueDriver {
    public static boolean main(String[] args) throws Exception {


        // Job 1: Join datasets
        Configuration joinConf = new Configuration();
        Job joinJob = Job.getInstance(joinConf, "Join Analysis and Metadata");
        joinJob.setJarByClass(UniqueDriver.class);
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


        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Average Song Segments");
        job.setJarByClass(UniqueDriver.class);
        job.setMapperClass(SegmentDataMapper.class);
        job.setReducerClass(SegmentDataReducer.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        
        FileInputFormat.addInputPath(job, new Path(args[2]));
        FileOutputFormat.setOutputPath(job, new Path(args[3]));

        if (!job.waitForCompletion(true)) {
            System.err.println("Join job failed, exiting...");
            System.exit(1);
        }



        Configuration conf3 = new Configuration();
        Job job3 = Job.getInstance(conf3, "vector average");

        job3.addCacheFile(new URI("hdfs://raleigh.cs.colostate.edu:31640/ms/q7/job_two/part-r-00000"));


        job3.setJarByClass(UniqueDriver.class);
        job3.setMapperClass(VectorAverageMapper.class);
        job3.setReducerClass(VectorAverageReducer.class);
        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job3, new Path(args[3]));
        FileOutputFormat.setOutputPath(job3, new Path(args[4]));

        if (!job3.waitForCompletion(true)) {
            System.err.println("Join job failed, exiting...");
            System.exit(1);
        }



        
        Configuration conf4 = new Configuration();
        Job job4 = Job.getInstance(conf4, "vector distance");

        job4.addCacheFile(new URI("hdfs://raleigh.cs.colostate.edu:31640/ms/q7/final_answer/part-r-00000"));


        job4.setJarByClass(UniqueDriver.class);
        job4.setInputFormatClass(KeyValueTextInputFormat.class);
        job4.setMapperClass(DistanceMapper.class);
        job4.setReducerClass(DistanceReducer.class);

        job4.setMapOutputKeyClass(Text.class);
        job4.setMapOutputValueClass(DoubleWritable.class);

        job4.setOutputKeyClass(Text.class);
        job4.setOutputValueClass(DoubleWritable.class);

        job4.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job4, new Path(args[4]));
        FileOutputFormat.setOutputPath(job4, new Path(args[5]));

        System.exit(job4.waitForCompletion(true) ? 0 : 1);

        return job4.waitForCompletion(true);

    }
}
