package sougoudata;

import org.apache.commons.lang.ObjectUtils;
import org.apache.hadoop.io.*;

import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


public class SougouQMapper extends  Mapper<LongWritable,Text,Text,NullWritable>{
    protected void map(LongWritable key1,Text value1,Context context) throws IOException, InterruptedException{

        //String data =value1.toString();

        String data = new String(value1.getBytes(),0,value1.getLength(),"GBK");
        String[] word=data.split("\\s+");

        if (word.length != 6) {
            return;
        }
        String newData = data.replaceAll("\\s+", ",");
        String words[] = newData.split(",");

        try {
            if(Integer.parseInt(words[3])==2 && Integer.parseInt(words[4])==1){
                context.write(new Text(newData),NullWritable.get());
            }
        } catch (NumberFormatException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }


    }


}
