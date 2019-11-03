package wc;

import org.apache.hadoop.io.*;

import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


public class WordCountMapper extends Mapper<LongWritable,Text,Text,IntWritable>{
    protected void map(LongWritable key1,Text value1,Context context) throws IOException, InterruptedException{

        String data=value1.toString();
        String[] words=data.split(" ");

        for (String w:words){
            context.write(new Text(w),new IntWritable(1));
        }

    }


}
