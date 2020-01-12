package KuduAPI;

import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.*;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class CreateTest {
    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(CreateTest.class);
    private static final String KUDU_MASTER = "node01:7051";

    public static void main(String[] args) {

        System.out.println("-----------------------------------------------");
        System.out.println("Will try to connect to Kudu master at " + KUDU_MASTER);
        System.out.println("Run with -DkuduMaster=myHost:port to override.");
        System.out.println("-----------------------------------------------");
        String tableName = "HourData";
        KuduClient client = new KuduClient.KuduClientBuilder(KUDU_MASTER).build();

        try {
            logger.info("------------create start--------------");
            //创建表
            List<ColumnSchema> columns = new ArrayList(4);
            columns.add(new ColumnSchema.ColumnSchemaBuilder("time", Type.STRING)
                    .key(true)
                    .build());
            columns.add(new ColumnSchema.ColumnSchemaBuilder("count", Type.INT64)
                    .build());
            columns.add(new ColumnSchema.ColumnSchemaBuilder("total", Type.DOUBLE)
                    .build());
            columns.add(new ColumnSchema.ColumnSchemaBuilder("avg", Type.DOUBLE)
                    .build());
            List<String> rangeKeys = new ArrayList<String>();
            rangeKeys.add("time");
            Schema schema = new Schema(columns);
            client.createTable(tableName, schema,
                    new CreateTableOptions().setRangePartitionColumns(rangeKeys));



        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                client.close();
            } catch (KuduException e) {
                e.printStackTrace();
            }
        }










    }
}
