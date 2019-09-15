package com.junenatte.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.Locale;

public class SelectDemo {
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
            Get get = new Get(Bytes.toBytes(rowKey));
            Result result = table.get(get);
            for (Cell cell : result.rawCells()) {
                System.out.println("rowKey:\t" + Arrays.toString(CellUtil.cloneRow(cell)));
                long timestamp = cell.getTimestamp();
                String date = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.CHINA).format(new Date(timestamp));
                System.out.println("timestamp:\t" + date);
                System.out.println("columnFamily:\t" + Arrays.toString(CellUtil.cloneFamily(cell)));
                System.out.println("columnName:\t" + Arrays.toString(CellUtil.cloneQualifier(cell)));
                System.out.println("value:\t" + Arrays.toString(CellUtil.cloneValue(cell)));
                System.out.println("查找成功...");
            }
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
