package cn.hsa.ims.mapReduce.mapjoin;

import cn.hsa.ims.mapReduce.tablecount.TableBean;
import cn.hsa.ims.mapReduce.tablecount.TableMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class MapJoinDriver {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException, URISyntaxException {
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

        job.setJarByClass(MapJoinDriver.class);

        job.setMapperClass(MapJoinMapper.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(TableBean.class);

        job.setOutputKeyClass(TableBean.class);
        job.setOutputValueClass(NullWritable.class);

        job.addCacheFile(new URI("/Library/Blue/Study/BigData/Hadoop/资料/11_input/tablecache/pd.txt"));
        job.setNumReduceTasks(0);


        FileInputFormat.setInputPaths(job, new Path("/Library/Blue/Study/BigData/Hadoop/资料/11_input/inputtable2"));
        FileOutputFormat.setOutputPath(job, new Path("/Users/blue/Documents/hadoop/out14"));

        System.exit(job.waitForCompletion(true) ? 1 : 0);
    }
}
