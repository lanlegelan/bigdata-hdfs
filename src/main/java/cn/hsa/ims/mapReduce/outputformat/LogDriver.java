package cn.hsa.ims.mapReduce.outputformat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class LogDriver {

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        final Configuration configuration = new Configuration();
        final Job job = Job.getInstance(configuration);

        job.setJarByClass(LogDriver.class);
        job.setMapperClass(LogMapper.class);
        job.setReducerClass(LogReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        job.setOutputFormatClass(LogOutPutFormat.class);

        FileInputFormat.setInputPaths(job, new Path("/Library/Blue/Study/BigData/Hadoop/资料/11_input/inputoutputformat/log.txt"));
        FileOutputFormat.setOutputPath(job,new Path("/Users/blue/Documents/hadoop/out99"));
        System.exit(job.waitForCompletion(true)?1:0);
    }
}
