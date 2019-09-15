package com.junenatte.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class CreateDemo {
    public static void main(String[] args) {
        Configuration conf = HBaseConfiguration.create();
        Connection conn = null;
        Admin admin = null;
        try {
            conn = ConnectionFactory.createConnection(conf);
            admin = conn.getAdmin();
            String tableName = "hbase_demo";
            String[] columnFamilies = {"info", "address"};
            TableName tn = TableName.valueOf(tableName);
            List<ColumnFamilyDescriptor> cfds = new ArrayList<>();
            if (admin.tableExists(tn)) {
                admin.disableTable(tn);
                admin.deleteTable(tn);
            }
            TableDescriptorBuilder tdb = TableDescriptorBuilder.newBuilder(tn);
            for (String columnFamily : columnFamilies) {
                ColumnFamilyDescriptorBuilder cfdb = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(columnFamily));
                ColumnFamilyDescriptor cfd = cfdb.build();
                cfds.add(cfd);
            }
            tdb.setColumnFamilies(cfds);
            TableDescriptor td = tdb.build();
            admin.createTable(td);
            System.out.println("创建成功...");
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if (null != admin)
                    admin.close();
                if (null != conn)
                    conn.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
