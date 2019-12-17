package Mov;

import com.alibaba.fastjson.JSON;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class ActorBestMoviesMapper extends Mapper<LongWritable,Text,MovieInfo,NullWritable>{
    protected void map(LongWritable key1,Text value1,Context context) throws IOException, InterruptedException{

        String json=value1.toString();
        //Fastjson 转换 json 字符串为 Java 对象
        MovieInfo movi= JSON.parseObject(json,MovieInfo.class);

        //威廉·达福
        if(movi.getActor().contains("威廉·达福")){
            context.write(movi,NullWritable.get());
        }else{}

    }


}
