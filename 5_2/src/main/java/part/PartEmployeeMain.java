package part;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class PartEmployeeMain {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {


        //创建一个job和任务入口
        Job job= Job.getInstance(new Configuration());
        job.setJarByClass(PartEmployeeMain.class);//main方法所在的class

        //指定job的mapper和输出类型<k2 v2>
        job.setMapperClass(PartEmployeeMapper.class);
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Employee.class);

        //指定任务的分区规则
        job.setPartitionerClass(MyEmployeePartitioner.class);
        //指定建立内几个分区
        job.setNumReduceTasks(3);

        //指定job的reduce和输出类型<k4 v4>
        job.setReducerClass(PartEmployeeReducer.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);



        //指定job的输入和输出
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        //执行job
        job.waitForCompletion(true);
    }
}
