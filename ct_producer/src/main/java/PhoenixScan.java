import java.sql.*;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
public class PhoenixScan {

    public static void main(String[] args) {
        query();
    }


    public static void query(){
        Connection conn = null;
        try {
            conn = PhoenixScan.getConnection();
            if (conn == null) {
                System.out.println("conn is null...");
                return;
            }
            String sql = "select * from test";
            PreparedStatement ps = conn.prepareStatement(sql);
            ResultSet rs = ps.executeQuery();

            System.out.println("state" + "\t" + "city" + "\t" + "population");
            System.out.println("======================");

            if (rs != null) {
                while (rs.next()) {
                    System.out.print(rs.getString("state") + "\t");
                    System.out.print(rs.getString("city") + "\t");
                    System.out.println(rs.getString("population"));
                }
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }finally {
            if(conn != null){
                try {
                    conn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }

    }


    public static Connection getConnection(){
        try {
            Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
            return DriverManager.getConnection("jdbc:phoenix:node01:2181");

        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();

        }
        return null;
    }


}
