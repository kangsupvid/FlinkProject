package KuduAPI;

import org.apache.kudu.client.*;

public class SelectTest {
    private static final String KUDU_MASTER = "node01:7051";
    public static void main(String[] args) {

        String tableName = "produces";
        KuduClient client = new KuduClient.KuduClientBuilder(KUDU_MASTER).build();

        try {
            //扫描数据
            long start = System.currentTimeMillis();

            KuduTable table = client.openTable(tableName);
            KuduPredicate predicate = KuduPredicate.newComparisonPredicate(
                    table.getSchema().getColumn("id"), KuduPredicate.ComparisonOp.EQUAL, 0
            );
            KuduScanner.KuduScannerBuilder builder = client.newScannerBuilder(table);
            builder.addPredicate(predicate);
            KuduScanner scanner = builder.build();
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
            long stop = System.currentTimeMillis();
            System.out.println("time：" + (stop - start));

            scanner.close();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                client.close();
            } catch (KuduException e) {
                e.printStackTrace();
            }
        }


    }}
