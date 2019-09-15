package com.junenatte.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class DeleteDemo {
    public static void main(String[] args) {
        Configuration conf = HBaseConfiguration.create();
        Connection conn = null;
        Table table = null;
        try {
            conn = ConnectionFactory.createConnection(conf);
            String tableName = "hbase_demo";
            TableName tn = TableName.valueOf(tableName);
            table = conn.getTable(tn);
            String rowKey = "1";
            String columnFamily = "info";
            Delete delete = new Delete(Bytes.toBytes(rowKey));
            delete.addFamily(Bytes.toBytes(columnFamily));
            table.delete(delete);
            System.out.println("删除成功...");
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
