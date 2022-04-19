package cn.hsa.ims.mapReduce.outputformat;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class LogRecordWriter extends RecordWriter<Text, NullWritable> {

    private static final Logger LOGGER = LoggerFactory.getLogger(LogRecordWriter.class);
    private  FSDataOutputStream atFS;
    private  FSDataOutputStream otherFs;


    public LogRecordWriter(TaskAttemptContext job) {
        try {
            FileSystem fileSystem = FileSystem.get(job.getConfiguration());

            atFS = fileSystem.create(new Path("/Users/blue/Documents/hadoop/out12/atguigu.log"));
            otherFs = fileSystem.create(new Path("/Users/blue/Documents/hadoop/out12/other.log"));
        } catch (IOException e) {
            LOGGER.error("系统异常",e);
        }
    }

    @Override
    public void write(Text key, NullWritable value) throws IOException, InterruptedException {
        final String line = key.toString();
        if(line.contains("atguigu")){
            atFS.writeBytes(line+"\n");
        }else {
            otherFs.writeBytes(line+"\n");
        }

    }

    @Override
    public void close(TaskAttemptContext context) throws IOException, InterruptedException {
        atFS.close();
        otherFs.close();
    }
}
