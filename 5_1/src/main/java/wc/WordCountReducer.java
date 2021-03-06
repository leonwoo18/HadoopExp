package wc;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import java.io.IOException;

public class WordCountReducer extends Reducer<Text, IntWritable,Text,IntWritable>{
    protected void reduce (Text k3,Iterable<IntWritable> v3,Context context)throws IOException,InterruptedException{

        int total=0;
        for(IntWritable v:v3){
            total +=v.get();
        }

        context.write(k3,new IntWritable(total));
    }
}
