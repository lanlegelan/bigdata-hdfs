package cn.hsa.ims.yarn.tool;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.util.Arrays;

public class WordCountDriver {

    private static Tool tool;

    public static void main(String[] args) throws Exception {
        //创建配置文件
        Configuration conf = new Configuration();

        switch (args[0]){
            case "WordCount":
                tool=new WordCountTool();
                break;
            default:
                throw new RuntimeException("No such tool: "+args[0]);
        }
        System.exit(ToolRunner.run(conf,tool,Arrays.copyOfRange(args, 1, args.length)));
    }
}
