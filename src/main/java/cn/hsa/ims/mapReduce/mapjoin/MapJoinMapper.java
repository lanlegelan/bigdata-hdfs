package cn.hsa.ims.mapReduce.mapjoin;

import com.ctc.wstx.util.StringUtil;
import com.google.common.collect.Maps;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;

public class MapJoinMapper extends Mapper<LongWritable, Text,Text, NullWritable> {

    private static final HashMap<String, String> PD_MAP = Maps.newHashMap();

    private Text outV=new Text();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        URI[] cacheFiles = context.getCacheFiles();
        Path path = new Path(cacheFiles[0]);
        FileSystem fileSystem = FileSystem.get(context.getConfiguration());
        FSDataInputStream open = fileSystem.open(path);
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(open,"UTF-8"));
        String line;
        while (StringUtils.isNotEmpty(line=bufferedReader.readLine())){
            String[] split = line.split("\t");
            PD_MAP.put(split[0],split[1]);
        }
    }

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, NullWritable>.Context context) throws IOException, InterruptedException {
        String[] split = value.toString().split("\t");

        outV.set(split[0]+"\t"+PD_MAP.get(split[1])+"\t"+split[2]);

        context.write(outV,NullWritable.get());
    }
}
