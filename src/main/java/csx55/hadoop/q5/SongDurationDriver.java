package csx55.hadoop.q5;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import csx55.hadoop.common.DescendingIntWritableComparator;

public class SongDurationDriver {
    public static boolean main(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: SongDurationDriver <input path> <output path>");
            System.exit(-1);
        }


        Configuration conf1 = new Configuration();
        Job job1 = Job.getInstance(conf1, "Calculate Shortest");
        job1.setJarByClass(SongDurationDriver.class);
        job1.setMapperClass(FreqMapper.class);
        job1.setReducerClass(FreqReducer.class);

        job1.setMapOutputKeyClass(IntWritable.class);
        job1.setMapOutputValueClass(FloatWritable.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(FloatWritable.class);

        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1]));
        boolean job1Completed = job1.waitForCompletion(true);

        if (!job1Completed) {
            System.err.println("Error with job1");
            System.exit(1);
        }


        // Configure and setup the first job for finding extremes (min and max)
        Configuration conf2 = new Configuration();

        Job job2 = Job.getInstance(conf2, "Multi Input Example");
        job2.setJarByClass(SongDurationDriver.class);
        job2.setReducerClass(MatchReducer.class);
        job2.setOutputKeyClass(FloatWritable.class);
        job2.setOutputValueClass(Text.class);

        MultipleInputs.addInputPath(job2, new Path(args[0]), TextInputFormat.class, SongMapper.class);
        MultipleInputs.addInputPath(job2, new Path(args[1]), TextInputFormat.class, StatMapper.class);
        FileOutputFormat.setOutputPath(job2, new Path(args[2]));

        return job2.waitForCompletion(true);
    }
}
