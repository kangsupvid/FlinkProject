package Util;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import ut.DateUtils;
import ut.StringUtils;

import java.text.DecimalFormat;
import java.util.Properties;

public class kafka_realTIme {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "node01:9092");//kafka集群，broker-list
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        Producer<String, String> producer = new KafkaProducer<String, String>(props);


        java.util.Random random = new java.util.Random();
        String[] locations = new String[]{"山东", "河北", "北京", "天津", "四川", "山西", "广东", "广西", "贵州", "陕西"};

        for (int i=0;i<3;i++){

//            String date = DateUtils.getTodayDate();
//            String baseActionTime = date + " " + StringUtils.fulfuill(random.nextInt(24) + "");
//
//            String timeStamp1 = baseActionTime + ":" + StringUtils.fulfuill(random.nextInt(60) + "") + ":" + StringUtils.fulfuill(random.nextInt(60) + "");
//
////            String timeStamp =String.valueOf(new Date(timeStamp1).getTime());
//

            long timeStamp = DateUtils.getStringToDate(DateUtils.randomDate());
            String uid = StringUtils.fulfuill(4, random.nextInt(1000) + "");
            String pid = StringUtils.fulfuill(6, random.nextInt(100000) + "");

            String position = locations[random.nextInt(10)];
            DecimalFormat df = new DecimalFormat("#.00");
            double price = Double.parseDouble(df.format(random.nextDouble() * 1000));

            String v1 =  uid+" "+ pid+ " " + position + " " + price + " " + DateUtils.getDateToString(timeStamp) +  "- "+timeStamp ;
            String v =  uid+" "+ pid+ " " + position + " " + price + " " + timeStamp;
            System.out.println(v1);
            producer.send(new ProducerRecord<String, String>("abc ",0 ,"",v));
        }
        DBUtil.closeAll();
        producer.close();
    }

    }

