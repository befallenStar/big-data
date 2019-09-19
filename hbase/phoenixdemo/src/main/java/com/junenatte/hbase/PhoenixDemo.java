package com.junenatte.hbase;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class PhoenixDemo {

    public static void main(String[] args) {
		Connection conn = null;
		Statement stmt = null;
		ResultSet rs = null;
		String driverClass="org.apache.phoenix.jdbc.PhoenixDriver";
		String url = "jdbc:phoenix:master.hadoop.sjh,slave1.hadoop.sjh,slave2.hadoop.sjh:2181";
        try {
            Class.forName(driverClass);
            //PreparedStatement ps = null;//推荐首选
            conn = DriverManager.getConnection(url);
            stmt = conn.createStatement();
            rs = stmt.executeQuery("SELECT HOST,DATE,RESPONSE_TIME,GC_TIME,CPU_TIME,IO_TIME FROM SERVER_METRICS");
            System.out.println("HOST\tDATE\tRESPONSE_TIME\tGC_TIME\tCPU_TIME\tIO_TIME");
            while (rs.next()) {
                System.out.println(rs.getString(1) + "\t" + rs.getString(2) + "\t" + rs.getString(3) + "\t" + rs.getString(4) + "\t" + rs.getString(5) + "\t" + rs.getString(6));
            }
        } catch (SQLException | ClassNotFoundException e) {
            e.printStackTrace();
        } finally {
            try {
                if (null != rs)
                    rs.close();
                if (null != stmt)
                    stmt.close();
                if (null != conn)
                    conn.close();
            } catch (SQLException ex) {
                ex.printStackTrace();
            }
        }

    }

}
