package part;

import org.apache.hadoop.io.*;

import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


public class PartEmployeeMapper extends Mapper<LongWritable,Text,NullWritable,Employee>{
    protected void map(LongWritable key1,Text value1,Context context) throws IOException, InterruptedException{

        String data=value1.toString();
        String[] words=data.split(",");

        Employee e=new Employee();

        e.setEmpno(Integer.parseInt(words[0]));
        e.setEname(words[1]);
        e.setJob(words[2]);
        try {
            e.setMgr(Integer.parseInt(words[3]));
        } catch (Exception ex) {
            e.setMgr(-1);
        }
        e.setHiredate(words[4]);
        e.setSal(Integer.parseInt(words[5]));
        try {
            e.setComm(Integer.parseInt(words[6]));
        } catch (Exception ex) {
            e.setComm(0);
        }
        e.setDeptno(Integer.parseInt(words[7]));

        //因为最后输出的是员工数据，由v4输出即可，k4为空，所以此处的k2也为空
        NullWritable k2 = NullWritable.get();

        context.write(k2,e);


    }


}

