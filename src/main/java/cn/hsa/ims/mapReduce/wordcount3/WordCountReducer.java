package cn.hsa.ims.mapReduce.wordcount3;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


/**
 * KEYIN,reduce阶段输入的key的类型，Text
 * VALUEIN,reduce阶段输入的value类型，IntWritable
 * KEYOUT,reduce阶段输出的key的类型，Text
 * VALUEOUT,reduce阶段输出的value的类型，IntWritable
 */
public class WordCountReducer extends Reducer<Text, IntWritable,Text,IntWritable> {

    private int sum;
    private IntWritable v=new IntWritable(1);

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
        sum=0;
        values.forEach(intWritable -> {
            sum+=intWritable.get();
        });
        v.set(sum);
        context.write(key,v);
    }
}

