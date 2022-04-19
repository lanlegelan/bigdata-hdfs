package cn.hsa.ims.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;

public class HdfsClient {

    private FileSystem fs;

    @Before
    public void init() throws URISyntaxException, IOException, InterruptedException {
        //1 链接的集群nn地址
        URI uri = new URI("hdfs://hadoop102:8020");

        //2 创建一个配置文件
        Configuration configuration = new Configuration();

        //用户
        String user ="blue";

        //3 获取到客户端对象
        fs = FileSystem.get(uri, configuration,user);
    }

    @After
    public void close() throws IOException {
        fs.close();
    }

    //创建目录
    @Test
    public  void testMkdir() throws URISyntaxException, IOException, InterruptedException {

        //4 创建一个文件夹
        fs.mkdirs(new Path("/xiyou/huaguoshan1"));

        //5 关闭
        fs.close();
    }

    //上传操作
    @Test
    public void testPut() throws IOException {
        // 参数解读 ：参数一：表示删除原数据；参数二：是否允许覆盖；参数三：原数据路径；参数四：目标地址路径
        fs.copyFromLocalFile(false,false,new Path("/Library/WorkSpace/Project/bigdata/HDFSClinet/target/HDFSClinet-1.0-SNAPSHOT.jar"),new Path("/tool/HDFSClinet-1.0-SNAPSHOT.jar"));
    }

    //下载操作
    @Test
    public void testGet() throws IOException {
        // 参数解读：参数一：源文件是否删除；参数二：源文件路径HDFS；参数三：目标地址路径Win；参数四
        fs.copyToLocalFile(true,new Path("hdfs://hadoop102/xiyou/huaguoshan"),new Path("/Library/Blue/Study/BigData/Hadoop/"),false);
    }

    @Test
    public void testRm() throws IOException {
        //参数解读： 参数一：要删除的路径；参数二：是否递归删除
        //删除文件
        //fs.delete(new Path("hdfs://hadoop102/xiyou/huaguoshan1/hadoop-3.1.3.tar.gz"),false);

        //删除空目录
       // fs.delete(new Path("/xiyou/huaguoshan1"),false);

        //删除非空目录
        fs.delete(new Path("/xiyou"),true);
    }

    //文件的更名和移动
    @Test
    public void testMv() throws IOException {
        //参数解读：参数一：源文件路径；参数二：目标文件路径
        //对文件名称的修改
        //fs.rename(new Path("/input/weiguo.txt"),new Path("/input/tutu.txt"));

        //文件的移动和更名
//        fs.rename(new Path("/input/tutu.txt"),new Path("/laotu.txt"));
        
        //目录的更名
        fs.rename(new Path("/input"),new Path("/output"));
    }
    
    //查看文件的详情信息
    @Test
    public void fileDetail() throws IOException {
        RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/"), true);
        //遍历文件
        while (listFiles.hasNext()) {
            LocatedFileStatus fileStatus =  listFiles.next();
            System.out.println("=========="+fileStatus.getPath()+"=========");
            System.out.println(fileStatus.getPermission());
            System.out.println(fileStatus.getOwner());
            System.out.println(fileStatus.getGroup());
            System.out.println(fileStatus.getLen());
            System.out.println(fileStatus.getModificationTime());
            System.out.println(fileStatus.getReplication());
            System.out.println(fileStatus.getBlockSize());
            System.out.println(fileStatus.getPath().getName());
            // 获取块信息
            BlockLocation[] blockLocations = fileStatus.getBlockLocations();
            System.out.println(Arrays.toString(blockLocations));

        }
    }

    //判断是文件夹还是文件
    @Test
    public void testFlie() throws IOException {

        FileStatus[] fileStatuses = fs.listStatus(new Path("/"));
        for (FileStatus fileStatus : fileStatuses) {

            if(fileStatus.isFile()){
                System.out.println("文件:"+fileStatus.getPath().getName());
            }else {
                System.out.println("目录:"+fileStatus.getPath().getName());
            }
        }
    }
            
}
