package hadoop.ch03.v17034460134;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import java.net.URI;



/**
 * HDFS ： Create File
 */
public class UploadHDFSFile{
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
// 配置 NameNode 地址，具体根据你的NameNode IP 配置
        URI uri = new URI("hdfs://192.168.30.131:9000");
// 指定用户名 , 获取 FileSystem 对象
        FileSystem fs = FileSystem.get(uri, conf, "hadoop");
//定义文件路径
        Path src = new Path("O:\\txt4.txt");
        Path dts = new Path("/17034460134/test4.txt");
        fs.copyFromLocalFile(src,dts);
        fs.close();
    }
}
