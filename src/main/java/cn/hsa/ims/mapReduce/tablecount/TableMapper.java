package cn.hsa.ims.mapReduce.tablecount;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class TableMapper extends Mapper<LongWritable, Text,Text,TableBean> {

    private String fileName;
    private TableBean outV=new TableBean();
    private Text outK=new Text();

    @Override
    protected void setup(Mapper<LongWritable, Text, Text, TableBean>.Context context) throws IOException, InterruptedException {
        FileSplit inputSplit = (FileSplit)context.getInputSplit();
        fileName = inputSplit.getPath().getName();
    }

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, TableBean>.Context context) throws IOException, InterruptedException {
        String[] split = value.toString().split("\t");
        if(fileName.contains("order")){
            for (String s : split) {
                outK.set(split[1]);
                outV.setId(split[0]);
                outV.setPid(split[1]);
                outV.setAmount(Integer.parseInt(split[2]));
                outV.setPname("");
                outV.setFlag("order");
            }
        }else {
            for (String s : split) {
                outK.set(split[0]);
                outV.setId("");
                outV.setPid(split[0]);
                outV.setAmount(0);
                outV.setPname(split[1]);
                outV.setFlag("pd");
            }
        }
        context.write(outK,outV);
    }
}
