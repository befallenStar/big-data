package com.junenatte.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class InsertDemo {
    public static void main(String[] args) {
        Configuration conf = HBaseConfiguration.create();
        Connection conn = null;
        Table table = null;
        try {
            conn = ConnectionFactory.createConnection(conf);
            String tableName = "hbase_demo";
            String[] columnFamilies = {"info", "address"};
            String[] columnOfInfo = {"name", "age"};
            String[] columnOfAddress = {"prov", "city"};
            String[] valueLines = {"zhangsan,18,jiangsu,nanjing", "lisi,20,sichuan,chengdu"};
            List<Put> puts = new ArrayList<>();
            TableName tn = TableName.valueOf(tableName);
            table = conn.getTable(tn);
            String count = "1";
            for (String value : valueLines) {
                String[] sp = value.split(",");
                Put put = new Put(Bytes.toBytes(count));
                for (int i = 0; i < columnOfInfo.length; i++)
                    put.addColumn(Bytes.toBytes(columnFamilies[0]), Bytes.toBytes(columnOfInfo[i]), Bytes.toBytes(sp[i]));
                for (int i = 0; i < columnOfAddress.length; i++)
                    put.addColumn(Bytes.toBytes(columnFamilies[1]), Bytes.toBytes(columnOfAddress[i]), Bytes.toBytes(sp[i + columnOfInfo.length]));
                puts.add(put);
                int c=Integer.parseInt(count);
                count=String.valueOf(++c);
            }
            table.put(puts);
            System.out.println("添加成功...");
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if (null != table)
                    table.close();
                if (null != conn)
                    conn.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
