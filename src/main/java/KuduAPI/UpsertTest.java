package KuduAPI;

import org.apache.kudu.client.*;
import org.slf4j.LoggerFactory;

public class UpsertTest {
    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(UpsertTest.class);
    private static final String KUDU_MASTER = "node01:7051";

    public static void main(String[] args) {

        System.out.println("-----------------------------------------------");
        System.out.println("Will try to connect to Kudu master at " + KUDU_MASTER);
        System.out.println("Run with -DkuduMaster=myHost:port to override.");
        System.out.println("-----------------------------------------------");
        String tableName = "kuduTest";
        KuduClient client = new KuduClient.KuduClientBuilder(KUDU_MASTER).build();

        try {

            //向表内插入新数据
            KuduTable table = client.openTable(tableName);
            KuduSession session = client.newSession();
            session.setTimeoutMillis(60000);

            logger.info("------------update start--------------");
            //更新数据
            //Update update = table.newUpdate();
            Upsert upsert = table.newUpsert();
//            PartialRow row1 = update.getRow();
            PartialRow row1 = upsert.getRow();
            row1.addInt("key",3);
            row1.addString("value","aaa");
            session.apply(upsert);


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
