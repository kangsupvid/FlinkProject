

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.Random;

/*
@ClassName:com.likang.util
        *@Description:TODO(页面跳转controller)
        *@author 立康
        *@date --:
*/
public class PKKafkaProducer {
    public static void main(String[] args) {
        final Logger log = LoggerFactory.getLogger(PKKafkaProducer.class);
        final Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.20.31:9092");
        props.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);

        String topic = "example";

        //通过死循环一直不停往kafka的broker里面生产数据
        while(true){
            StringBuilder builder = new StringBuilder();
            builder.append("imooc").append("\t")
                    .append("CN").append("\t")
                    .append(getLevels()).append("\t")
                    .append(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date())).append("\t")
                    .append(getIps()).append("\t")//得到ip
                    .append(getDomains()).append("\t")//得到域名
                    .append(gettraffic());//得到域名
            System.out.println(builder.toString());
            producer.send(new ProducerRecord<String,String>(topic,builder.toString()));
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }



    }

    private static int gettraffic() {
        return new Random().nextInt(10000);
    }

    private static String getDomains() {
        String[] domains = new String[]{
                "aiqiyi.edu.cn",
                "likang.edu.cn",
                "yahooh.com.cn",
                "baidue.edu.cn",
                "github.edu.cn"
        };
        return domains[new Random().nextInt(domains.length)];
    }

    private static String getIps() {
        String[] ips = new String[]{
                "223.104.20.110",
                "112.111.56.134",
                "113.145.25.123",
                "214.112.20.124",
                "114.166.13.112",
                "245.234.23.165"
        };

        return ips[new Random().nextInt(ips.length)];
    }

    public static String getLevels(){
        String[] levels = new String[]{"M","E"};
        return levels[new Random().nextInt(levels.length)];
    }
}
