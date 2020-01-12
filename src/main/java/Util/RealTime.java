package Util;

import ut.DateUtils;
import ut.StringUtils;

import java.text.DecimalFormat;


public class RealTime {
            public static void main(String[] args) {
                java.util.Random random = new java.util.Random();
                String[] locations = new String[]{"山东", "河北", "北京", "天津", "四川", "山西", "广东", "广西", "贵州", "陕西"};

                while (true) {

                    String date = DateUtils.getTodayDate();
//                    String baseActionTime = date + " " + StringUtils.fulfuill(random.nextInt(24) + "");
                    String baseActionTime = date + " " + 07 + "";
                    String timeStamp = baseActionTime + ":" + StringUtils.fulfuill(random.nextInt(60) + "") + ":" + StringUtils.fulfuill(random.nextInt(60) + "");
//                    String timeStamp = Long.toString(new Date().getTime());
                    String uid = StringUtils.fulfuill(4, random.nextInt(1000) + "");
                    String pid = StringUtils.fulfuill(6, random.nextInt(100000) + "");

                    String position = locations[random.nextInt(10)];
                    DecimalFormat df = new DecimalFormat("#.00");
                    double price = Double.parseDouble(df.format(random.nextDouble() * 1000));


                    String sql = "insert  into   product  values(?,?,?,?,?)";

                    Object[] obj = {uid, pid, position, price, timeStamp};
                    System.out.println(uid + " " + pid + " " + position + " " + price + " " + timeStamp);
                    DBUtil.executeUpdate(sql, obj);

                    DBUtil.closeAll();


            }

    }



}



