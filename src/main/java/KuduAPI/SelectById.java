package KuduAPI;

import bean.Information;
import org.apache.kudu.client.*;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class SelectById implements Serializable {
    private static final long serialVersionUID = -2030801981787749120L;


    public List<Information> run(String KUDU_MASTER,String tableName,String column,Object o) {
        ArrayList<Information> list = new ArrayList<Information>();

        KuduClient client = new KuduClient.KuduClientBuilder(KUDU_MASTER).build();

        try {
            //扫描数据
            long start = System.currentTimeMillis();

            KuduTable table = client.openTable(tableName);

            int cont = 0;
            KuduPredicate predicate = KuduPredicate.newComparisonPredicate(
                    table.getSchema().getColumn(column), KuduPredicate.ComparisonOp.EQUAL, String.valueOf(o)
            );
            KuduScanner.KuduScannerBuilder builder = client.newScannerBuilder(table);
            builder.addPredicate(predicate);
            KuduScanner scanner = builder.build();
            while (scanner.hasMoreRows()) {
                RowResultIterator results = scanner.nextRows();
                while (results.hasNext()) {
                    RowResult result = results.next();
                    Information information = new Information(result.getInt(0), result.getLong(1), result.getDouble(2));
                    list.add(information);
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
        return list;
    }


}