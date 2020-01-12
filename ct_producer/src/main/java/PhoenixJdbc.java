import java.sql.*;

/**
 * Created by blockchain on 18-9-5 上午10:34 in Beijing.
 */

public class PhoenixJdbc {
    public static void main(String[] args) throws ClassNotFoundException, SQLException {

        Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");

        //这里配置zookeeper的地址 可以是域名或者ip 可单个或者多个(用","分隔)
        String url = "jdbc:phoenix:node01:2181";
        Connection conn = DriverManager.getConnection(url);
        Statement statement = conn.createStatement();

        ResultSet rs = statement.executeQuery("select * from TEST");
        while (rs.next()) {
            int pk = rs.getInt("PK");
            String col1 = rs.getString("COL1");

            System.out.println("PK=" + pk + ", COL1=" + col1);
        }
        // 关闭连接
        rs.close();

        statement.close();
        conn.close();
    }
}
