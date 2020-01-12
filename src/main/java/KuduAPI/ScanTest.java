package KuduAPI;

import org.apache.kudu.client.*;
import org.slf4j.LoggerFactory;
import java.util.ArrayList;
import java.util.List;

public class ScanTest {
    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(ScanTest.class);
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

            logger.info("------------scan start--------------");
            //扫描数据
            List<String> projectColumns = new ArrayList<String>(1);
            //projectColumns.add("value");
            KuduScanner scanner = client.newScannerBuilder(table)
                    //.setProjectedColumnNames(projectColumns)
                    .build();
            int cont = 0;
            while (scanner.hasMoreRows()) {
                RowResultIterator results = scanner.nextRows();
                while (results.hasNext()) {
                    RowResult result = results.next();
                    System.out.println(result.rowToString());
                    cont++;
                }
            }
            System.out.println("Number of rows: " + cont);

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
