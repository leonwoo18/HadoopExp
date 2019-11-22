package hadoop.chp06.v17034460134;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.ArrayList;
import java.util.Map;


public class HBaseDemo63 {

    private static Connection getConno() throws Exception{
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "node1,node2,node3");
        conf.set("hbase.rootdir", "hdfs://node1:9000/hbase");
        conf.set("hbase.cluster.distributed", "true");
        Connection connect = ConnectionFactory.createConnection(conf);
        return  connect;
    }

    public static void main(String[] args) throws Exception{
        createTable();
        insert();
        get();
        scan();
    }
    private static void createTable() throws Exception{
        Connection connect=getConno();
        Admin admin = connect.getAdmin();
        TableName tn = TableName.valueOf("emp17034460134");
        String[] family=new String[] {"id","address","info"};
        HTableDescriptor desc = new HTableDescriptor(tn);
        for (int i = 0; i < family.length; i++) {
            desc.addFamily(new HColumnDescriptor(family[i]));
        }
        if (admin.tableExists(tn)) {
            System.out.println("table Exists!");
            System.exit(0);
        }else{
            admin.createTable(desc);
            System.out.println("create table Success!");
          }
        }

        private static void insert() throws Exception{
            Table table=getConno().getTable(TableName.valueOf("emp17034460134"));
            //rowkey
            Put put = new Put(Bytes.toBytes("Rain"));
            //列族，列，值
           put.addColumn(Bytes.toBytes("id"),Bytes.toBytes(" "),Bytes.toBytes("31"));
           put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("age"),Bytes.toBytes("28"));
            put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("birthday"),Bytes.toBytes("1990-05-01"));
            put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("industry"),Bytes.toBytes("architect"));
            put.addColumn(Bytes.toBytes("address"),Bytes.toBytes("city"),Bytes.toBytes("ShenZhen"));
            put.addColumn(Bytes.toBytes("address"),Bytes.toBytes("country"),Bytes.toBytes("China"));
           table.put(put);
        }

        private static void get() throws Exception{
            Table table=getConno().getTable(TableName.valueOf("emp17034460134"));
            Get get=new Get(Bytes.toBytes("Rain"));
            Result result=table.get(get);
            ArrayList<String> cols = new ArrayList<String>();
            Map<byte[], byte[]> familyMap = result.getFamilyMap(Bytes.toBytes("info"));
            for(Map.Entry<byte[], byte[]> entry:familyMap.entrySet()){
                cols.add(Bytes.toString(entry.getKey()));
            }
            for (int i=0;i<cols.size();i++){
                byte[] name = result.getValue(Bytes.toBytes("info"), Bytes.toBytes(cols.get(i)));
                System.out.println(Bytes.toString(name));
            }
        }

        private static void scan() throws Exception{
            Table table=getConno().getTable(TableName.valueOf("emp17034460134"));
            Scan scan = new Scan();
            ResultScanner resultScanner = table.getScanner(scan);
            for (Result result : resultScanner) {
                byte[] name = result.getValue(Bytes.toBytes("info"), Bytes.toBytes("birthday"));
                System.out.println(Bytes.toString(name));
            }
        }
}
