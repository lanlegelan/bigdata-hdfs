package cn.hsa.ims.mapReduce.combinerWordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * KEYIN,map阶段输入的key的类型，LongWritable
 * VALUEIN,map阶段输入的value类型，Text
 * KEYOUT,map阶段输出的key的类型，Text
 * VALUEOUT,map阶段输出的value的类型，IntWritable
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private Text t=new Text();
    private IntWritable v=new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {

        //获取一行
        String s = value.toString();
        
        //切割并输出
        for (String word : s.split(" ")) {
            t.set(word);
            context.write(t,v);
        }


    }
}
