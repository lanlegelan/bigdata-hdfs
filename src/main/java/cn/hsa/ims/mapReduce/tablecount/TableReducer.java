package cn.hsa.ims.mapReduce.tablecount;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.commons.compress.utils.Lists;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.List;

public class TableReducer extends Reducer<Text,TableBean,TableBean, NullWritable> {

    @Override
    protected void reduce(Text key, Iterable<TableBean> values, Reducer<Text, TableBean, TableBean, NullWritable>.Context context) throws IOException, InterruptedException {
        List<TableBean> beanList= Lists.newArrayList();
        TableBean tableBean = new TableBean();
        for (TableBean value : values) {
            //创建临时bean
            TableBean tmpBean = new TableBean();
            if(value.getFlag().equals("order")){
                try {
                    BeanUtils.copyProperties(tmpBean,value);
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                } catch (InvocationTargetException e) {
                    e.printStackTrace();
                }
                beanList.add(tmpBean);
            }else {
                try {
                    BeanUtils.copyProperties(tableBean,value);
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                } catch (InvocationTargetException e) {
                    e.printStackTrace();
                }
            }
        }
        //遍历list，组装数据
        for (TableBean bean : beanList) {
            bean.setPname(tableBean.getPname());
            context.write(bean,NullWritable.get());
        }
    }
}
