package KuduAPI;

import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.*;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class Test {
    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(Test.class);
    private static final String KUDU_MASTER = "node01:7051";

    public static void main(String[] args) {

        System.out.println("-----------------------------------------------");
        System.out.println("Will try to connect to Kudu master at " + KUDU_MASTER);
        System.out.println("Run with -DkuduMaster=myHost:port to override.");
        System.out.println("-----------------------------------------------");
        String tableName = "kuduTest";
        KuduClient client = new KuduClient.KuduClientBuilder(KUDU_MASTER).build();

        try {
            logger.info("------------create start--------------");
            //创建表
            List<ColumnSchema> columns = new ArrayList(2);
            columns.add(new ColumnSchema.ColumnSchemaBuilder("key", Type.INT32)
                    .key(true)
                    .build());
            columns.add(new ColumnSchema.ColumnSchemaBuilder("value", Type.STRING)
                    .build());
            List<String> rangeKeys = new ArrayList<String>();
            rangeKeys.add("key");
            Schema schema = new Schema(columns);
            client.createTable(tableName, schema,
                    new CreateTableOptions().setRangePartitionColumns(rangeKeys));

            logger.info("------------insert start--------------");
//            client.getTablesList().getTablesList().forEach(str-> System.out.println(str));


//            //向表内插入新数据
            KuduTable table = client.openTable(tableName);
            KuduSession session = client.newSession();
            session.setTimeoutMillis(60000);
            for (int i = 0; i < 3; i++) {
                logger.info("----------------insert  "+i+"---------------");
                Insert insert = table.newInsert();
                PartialRow row = insert.getRow();
                row.addInt(0, i);
                row.addString(1, "value " + i);
                session.apply(insert);
            }

            logger.info("------------delete data start--------------");
            //根据主键删除数据
            Delete delete = table.newDelete();
            PartialRow row = delete.getRow();
            row.addInt("key",0);
            OperationResponse apply = session.apply(delete);
            if (apply.hasRowError()) {
                logger.info("------------delete fail--------------");
            } else {
                logger.info("------------delete success--------------");
            }

            logger.info("------------update start--------------");
            //更新数据
            Update update = table.newUpdate();
            PartialRow row1 = update.getRow();
            row1.addInt("key",6);
            row1.addString("value","kexin");
            session.apply(update);

            logger.info("------------scan start--------------");
            //扫描数据
            List<String> projectColumns = new ArrayList<String>(1);
            projectColumns.add("value");
            KuduScanner scanner = client.newScannerBuilder(table)
                    .setProjectedColumnNames(projectColumns)
                    .build();
            while (scanner.hasMoreRows()) {
                RowResultIterator results = scanner.nextRows();
                while (results.hasNext()) {
                    RowResult result = results.next();
                    System.out.println(result.getString(0));
                }
            }


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
