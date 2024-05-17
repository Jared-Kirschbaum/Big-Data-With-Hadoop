package csx55.hadoop.q7;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.net.URI;

public class SegmentDriver {
    public static boolean main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Average Song Segments");
        job.setJarByClass(SegmentDriver.class);
        job.setMapperClass(SegmentDataMapper.class);
        job.setReducerClass(SegmentDataReducer.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        if (!job.waitForCompletion(true)) {
            System.err.println("Join job failed, exiting...");
            System.exit(1);
        }


        Configuration conf2 = new Configuration();
        Job job2 = Job.getInstance(conf2, "Max Vector Length");
        job2.setJarByClass(SegmentDriver.class);
        job2.setMapperClass(VectorLengthMapper.class);
        job2.setReducerClass(MaxVectorLengthReducer.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job2, new Path(args[1]));
        FileOutputFormat.setOutputPath(job2, new Path(args[2]));

        if (!job2.waitForCompletion(true)) {
            System.err.println("Join job failed, exiting...");
            System.exit(1);
        }


        Configuration conf3 = new Configuration();
        Job job3 = Job.getInstance(conf3, "vector average");

        job3.addCacheFile(new URI("hdfs://raleigh.cs.colostate.edu:31640/ms/q7/job_two/part-r-00000#partFile"));


        job3.setJarByClass(SegmentDriver.class);
        job3.setMapperClass(VectorAverageMapper.class);
        job3.setReducerClass(VectorAverageReducer.class);
        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job3, new Path(args[1]));
        FileOutputFormat.setOutputPath(job3, new Path(args[3]));

        System.exit(job3.waitForCompletion(true) ? 0 : 1);

        return job3.waitForCompletion(true);
    }
}
