package cn.hsa.ims.mapReduce.writable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FlowReduce extends Reducer<Text,FlowBean,Text,FlowBean> {
    private FlowBean outF=new FlowBean();

    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Reducer<Text, FlowBean, Text, FlowBean>.Context context) throws IOException, InterruptedException {

        long totalUp=0;
        long totalDown=0;
        //遍历数据读取数据
        for (FlowBean flowBean : values) {
            totalUp+=flowBean.getUpFlow();
            totalDown+=flowBean.getDownFlow();
        }
        outF.setUpFlow(totalUp);
        outF.setDownFlow(totalDown);
        outF.setSumFlow();
        context.write(key,outF);

    }
}
