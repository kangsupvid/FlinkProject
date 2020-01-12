package KuduAPI;

import org.apache.kudu.client.*;
import org.slf4j.LoggerFactory;

public class UpdateTest {
    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(UpdateTest.class);
    private static final String KUDU_MASTER = "node01:7051";

    public static void main(String[] args) {

        System.out.println("-----------------------------------------------");
        System.out.println("Will try to connect to Kudu master at " + KUDU_MASTER);
        System.out.println("Run with -DkuduMaster=myHost:port to override.");
        System.out.println("-----------------------------------------------");
        String tableName = "product";
        KuduClient client = new KuduClient.KuduClientBuilder(KUDU_MASTER).build();

        try {

            //向表内插入新数据
            KuduTable table = client.openTable(tableName);
            KuduSession session = client.newSession();
            session.setTimeoutMillis(60000);

            logger.info("------------update start--------------");
            //更新数据
            Update update = table.newUpdate();
            PartialRow row1 = update.getRow();
            row1.addInt("id",0);
            row1.addLong("count",44);
            session.apply(update);


        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {

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
