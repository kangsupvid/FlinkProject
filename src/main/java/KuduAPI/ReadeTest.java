package KuduAPI;

import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduSession;
import org.apache.kudu.client.KuduTable;
import org.slf4j.LoggerFactory;

public class ReadeTest {
    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(ReadeTest.class);
    private static final String KUDU_MASTER = "node01:7051";

    public static void main(String[] args) {

        System.out.println("-----------------------------------------------");
        System.out.println("Will try to connect to Kudu master at " + KUDU_MASTER);
        System.out.println("Run with -DkuduMaster=myHost:port to override.");
        System.out.println("-----------------------------------------------");
        String tableName = "kuduTest";
        KuduClient client = new KuduClient.KuduClientBuilder(KUDU_MASTER).build();

        try {

            KuduTable table = client.openTable(tableName);
            KuduSession session = client.newSession();
            session.setTimeoutMillis(60000);




        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                logger.info("------------delete table start--------------");
                //删除表
                client.deleteTable(tableName);
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                try {
                    client.shutdown();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }










    }
}
