package com.junenatte.hbase.hbasemapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import java.io.IOException;

public class WordCountTest {
    public static void main(String[] args) {
        String tableName = "wordcount";
        String[] columnFamilies = {"content"};
        createTable(tableName,columnFamilies);
        try {
            Configuration conf = HBaseConfiguration.create();
            conf.set(TableOutputFormat.OUTPUT_TABLE, "wordcount");
            Job job = Job.getInstance(conf);
            job.setJarByClass(WordCountTest.class);
            job.setInputFormatClass(TextInputFormat.class);
            TextInputFormat.setInputPaths(job, new Path("/main/sjh/hbasemapreduce/*.txt"));

            job.setMapperClass(WordCountMapper.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(IntWritable.class);

            job.setReducerClass(WordCountReducer.class);
            job.setOutputFormatClass(TableOutputFormat.class);
            boolean b = job.waitForCompletion(true);
        } catch (InterruptedException | IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    private static void createTable(String tableName,String[] columnFamilies) {
        Configuration conf = HBaseConfiguration.create();
        Connection conn = null;
        Admin admin = null;
        try {
            conn = ConnectionFactory.createConnection(conf);
            admin = conn.getAdmin();
            TableName tn = TableName.valueOf(tableName);
            if (admin.tableExists(tn)) {
                admin.disableTable(tn);
                admin.deleteTable(tn);
            }
            TableDescriptorBuilder tdb = TableDescriptorBuilder.newBuilder(tn);
            for (String columnFamilyName : columnFamilies) {
                ColumnFamilyDescriptorBuilder cfdb = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(columnFamilyName));
                ColumnFamilyDescriptor cfd = cfdb.build();
                tdb.setColumnFamily(cfd);
            }
            TableDescriptor td = tdb.build();
            admin.createTable(td);
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
