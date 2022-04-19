package cn.hsa.ims.mapReduce.writableComparable;

import cn.hsa.ims.mapReduce.writable.ProvincePartition;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class FlowDriver {

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

        //获取job
        Configuration entries = new Configuration();
        Job job = Job.getInstance(entries);

        //设置jar包路径
        job.setJarByClass(FlowDriver.class);

        //设置map
        job.setMapperClass(FlowMapper.class);
        job.setReducerClass(FlowReduce.class);


        //设置Map输出的kv
        job.setMapOutputKeyClass(FlowBean.class);
        job.setMapOutputValueClass(Text.class);

        //设置最终输出的kv
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        //设置程序输出路径
        FileInputFormat.setInputPaths(job,new Path("/Users/blue/Documents/hadoop/out/part-r-00000"));
        FileOutputFormat.setOutputPath(job,new Path("/Users/blue/Documents/hadoop/out11111"));

        System.exit( job.waitForCompletion(true)?0:1);
    }
}
