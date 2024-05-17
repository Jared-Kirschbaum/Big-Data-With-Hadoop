package csx55.hadoop.q6;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class EnergyDanceDriver {
    public static boolean main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Top Energetic and Danceable Songs");
        job.setJarByClass(EnergyDanceDriver.class);
        job.setMapperClass(EnergyDanceMapper.class);
        job.setReducerClass(TopSongsReducer.class);
        job.setOutputKeyClass(FloatWritable.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        return job.waitForCompletion(true);
    }
}
