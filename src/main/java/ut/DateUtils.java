package ut;

import scala.util.Random;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**
 * 日期时间工具类
 * @author Administrator
 *
 */
public class DateUtils {

    public static final SimpleDateFormat TIME_FORMAT =
            new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    public static final SimpleDateFormat DATE_FORMAT =
            new SimpleDateFormat("yyyy-MM-dd");
    public static final SimpleDateFormat DATEKEY_FORMAT =
            new SimpleDateFormat("yyyyMMdd");

    /**
     * 判断一个时间是否在另一个时间之前
     * @param time1 第一个时间
     * @param time2 第二个时间
     * @return 判断结果
     */
    public static boolean before(String time1, String time2) {
        try {
            Date dateTime1 = TIME_FORMAT.parse(time1);
            Date dateTime2 = TIME_FORMAT.parse(time2);

            if(dateTime1.before(dateTime2)) {
                return true;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }

    /**
     * 判断一个时间是否在另一个时间之后
     * @param time1 第一个时间
     * @param time2 第二个时间
     * @return 判断结果
     */
    public static boolean after(String time1, String time2) {
        try {
            Date dateTime1 = TIME_FORMAT.parse(time1);
            Date dateTime2 = TIME_FORMAT.parse(time2);

            if(dateTime1.after(dateTime2)) {
                return true;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }

    /**
     * 计算时间差值（单位为秒）
     * @param time1 时间1
     * @param time2 时间2
     * @return 差值
     */
    public static int minus(String time1, String time2) {
        try {
            Date datetime1 = TIME_FORMAT.parse(time1);
            Date datetime2 = TIME_FORMAT.parse(time2);

            long millisecond = datetime1.getTime() - datetime2.getTime();

            return Integer.valueOf(String.valueOf(millisecond / 1000));
        } catch (Exception e) {
            e.printStackTrace();
        }
        return 0;
    }

    /**
     * 获取年月日和小时
     * @param datetime 时间（yyyy-MM-dd HH:mm:ss）
     * @return 结果（yyyy-MM-dd_HH）
     */
    public static String getDateHour(String datetime) {
        String date = datetime.split(" ")[0];
        String hourMinuteSecond = datetime.split(" ")[1];
        String hour = hourMinuteSecond.split(":")[0];
        return date + "_" + hour;
    }

    /**
     * 获取当天日期（yyyy-MM-dd）
     * @return 当天日期
     */
    public static String getTodayDate() {
        return DATE_FORMAT.format(new Date());
    }

    /**
     * 获取昨天的日期（yyyy-MM-dd）
     * @return 昨天的日期
     */
    public static String getYesterdayDate() {
        Calendar cal = Calendar.getInstance();
        cal.setTime(new Date());
        cal.add(Calendar.DAY_OF_YEAR, -1);

        Date date = cal.getTime();

        return DATE_FORMAT.format(date);
    }

    /**
     * 格式化日期（yyyy-MM-dd）
     * @param date Date对象
     * @return 格式化后的日期
     */
    public static String formatDate(Date date) {
        return DATE_FORMAT.format(date);
    }

    /**
     * 格式化时间（yyyy-MM-dd HH:mm:ss）
     * @param date Date对象
     * @return 格式化后的时间
     */
    public static String formatTime(Date date) {
        return TIME_FORMAT.format(date);
    }

    /**
     * 解析时间字符串
     * @param time 时间字符串
     * @return Date
     */
    public static Date parseTime(String time) {
        try {
            return TIME_FORMAT.parse(time);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 格式化日期key
     * @param date
     * @return  yyyyMMdd
     */
    public static String formatDateKey(Date date) {
        return DATEKEY_FORMAT.format(date);
    }

    /**
     * 格式化日期key	(yyyyMMdd)
     * @param datekey
     * @return
     */
    public static Date parseDateKey(String datekey) {
        try {
            return DATEKEY_FORMAT.parse(datekey);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 格式化时间，保留到分钟级别
     * 
     * @param date
     * @return  yyyyMMddHHmm --201907232301
     */
    public static String formatTimeMinute(Date date) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmm");
        return sdf.format(date);
    }
    
    /**
     * 
     * @param dateTime yyyy-MM-dd HH:mm:ss
     * @return
     */
	public static String getRangeTime(String dateTime) {
		 String date = dateTime.split(" ")[0];
		 String hour = dateTime.split(" ")[1].split(":")[0];
		 int minute = StringUtils.convertStringtoInt(dateTime.split(" ")[1].split(":")[1]);
//		 String second = dateTime.split(" ")[1].split(":")[2];
		 if(minute+(5-minute % 5) == 60){
			 return date+" "+hour+":"+StringUtils.fulfuill((minute-(minute % 5))+"")+"~"+date+" "+StringUtils.fulfuill((Integer.parseInt(hour)+1)+"")+":00";
		 }
		 return date+" "+hour+":"+StringUtils.fulfuill((minute-(minute % 5))+"") +"~" + date+" "+hour+":"+StringUtils.fulfuill((minute+(5-minute % 5))+"");
	}
    public static Long stringStamp2Date(String time){

        SimpleDateFormat format=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        Date date = null;
        try {
            date = format.parse(time);
        } catch (ParseException e) {
            e.printStackTrace();
        }

        Long timestamp=date.getTime();

        return timestamp;
    }

    public static String randomDate(){
        Random rndYear=new Random();
        int year=rndYear.nextInt(1)+2018;
        Random rndMonth=new Random();
        int month=rndMonth.nextInt(1)+11;
        Random rndDay=new Random();
        int Day=rndDay.nextInt(1)+27;
        Random rndHour=new Random();
        int hour=rndHour.nextInt(23);
        Random rndMinute=new Random();
        int minute=rndMinute.nextInt(60);
        Random rndSecond=new Random();
        int second=rndSecond.nextInt(60);
        return year+"-"+cp(month)+"-"+cp(Day)+"  "+cp(hour)+":"+cp(minute)+":"+cp(second);
    }
    private static String cp(int num){
        String Num=num+"";
        if (Num.length()==1){
            return "0"+Num;
        }else {
            return Num;
        }
    }

    /**
           * 将字符串转为时间戳
            */
    public static long getStringToDate(String time) {
        SimpleDateFormat sf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date date = new Date();
        try{
            date = sf.parse(time);
        } catch(ParseException e) {
            e.printStackTrace();
        }
        return date.getTime();
    }

    /*** 时间戳转换成字符串
          */
    public static String getDateToString(long time) {
        Date d = new Date(time);
        SimpleDateFormat sf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        return sf.format(d);
    }
    public static Long getTodayZeroPointTimestamps(Long now) {

        long daySecond = 60 * 60 * 24 * 1000;
        long dayTime = now - (now + 8 * 3600 * 1000) % daySecond + 1 * daySecond;
        return dayTime;
    }

    public static void main(String[] args) {
        long stringToDate = DateUtils.getStringToDate(DateUtils.randomDate());
        System.out.println(stringToDate);
    }
}
