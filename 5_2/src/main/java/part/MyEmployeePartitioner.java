package part;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class MyEmployeePartitioner extends Partitioner<NullWritable, Employee> {


    public int getPartition(NullWritable k2, Employee v2, int numPatition) {

        //如何分区:
        if (v2.getSal() < 1500) {
            //放入1号分区中
            return 1 % numPatition;
        } else if (v2.getSal() >= 1500 && v2.getSal() < 3000) {
            //放入2号分区中
            return 2 % numPatition;
        } else {
            //放入3号分区中
            return 3 % numPatition;
        }
    }
}