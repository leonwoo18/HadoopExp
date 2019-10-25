package hadoop.ch03.v17034460134;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.net.URI;

public class ReadHDFSFile {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        URI uri = new URI("hdfs://192.168.30.131:9000");// 配置NameNode地址
        FileSystem fs = FileSystem.get(uri, conf, "hadoop");// 指定用户名,获取FileSystem对象
        Path path = new Path("/17034460134/test5.txt");//定义文件路径


        FSDataInputStream dis = fs.open(path);
        String str = null;
        while ((str = dis.readLine()) != null) {


            System.out.print(str);
        }
        dis.close();
        fs.close();//关闭FileSystem
    }
}
