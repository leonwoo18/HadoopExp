package part;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import java.io.IOException;

public class PartEmployeeReducer extends Reducer<NullWritable, Employee,NullWritable,Text>{
    protected void reduce (NullWritable k3,Iterable<Employee> v3,Context context)throws IOException,InterruptedException{

        String line=null;
        for(Employee e:v3){
            line=e.toString();
            context.write(k3, new Text(line));
        }


    }
}
