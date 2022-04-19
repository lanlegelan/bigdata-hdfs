package cn.hsa.ims.mapReduce.writable;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


import java.io.IOException;

public class FlowMapper extends Mapper<LongWritable, Text, Text, FlowBean> {

    private Text outK=new Text();

    private FlowBean outF=new FlowBean();

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, FlowBean>.Context context) throws IOException, InterruptedException {
        String line=value.toString();
        String[] split = line.split("\t");

        String phone = split[1];
        String up = split[split.length - 3];
        String down = split[split.length - 2];
        outK.set(phone);
        outF.setUpFlow(Long.parseLong(up));
        outF.setDownFlow(Long.parseLong(down));
        outF.setSumFlow();
        context.write(outK,outF);
    }
}
