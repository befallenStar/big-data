package com.junenatte.hive;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class HiveDemo {
    public static void main(String[] args) {
        Connection conn = null;
        Statement stmt = null;
        ResultSet rs = null;
        try {
            Class.forName("org.apache.hive.jdbc.HiveDriver");
            conn = DriverManager.getConnection("jdbc:hive2://master.hadoop.sjh:10000/test", "hive", "123456");
            stmt = conn.createStatement();
            rs = stmt.executeQuery("select userid, name from myuser order by userid desc");
            System.out.println("userid\tname");
            while (rs.next()) {
                System.out.println(rs.getString(1) + "\t" + rs.getString(1));
            }
        } catch (ClassNotFoundException | SQLException e) {
            e.printStackTrace();
        } finally {
            try {
                if (null != rs)
                    rs.close();
                if (null != stmt)
                    stmt.close();
                if (null != conn)
                    conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
}
