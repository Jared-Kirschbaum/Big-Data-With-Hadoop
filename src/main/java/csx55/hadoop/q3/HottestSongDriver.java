package csx55.hadoop.q3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import csx55.hadoop.common.DescendingIntWritableComparator;

public class HottestSongDriver {
    public static boolean main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: HottestSongDriver <input path> <output path>");
            System.exit(-1);
        }
    
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Hottest Song Finder");
        job.setJarByClass(HottestSongDriver.class);
        job.setMapperClass(HottestSongMapper.class);
        job.setReducerClass(HottestSongReducer.class);
    
        job.setMapOutputKeyClass(FloatWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);

        job.setSortComparatorClass(DescendingIntWritableComparator.class);
    
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
    
        return job.waitForCompletion(true);
    }
    
}