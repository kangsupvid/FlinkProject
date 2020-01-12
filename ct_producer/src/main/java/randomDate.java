import java.text.DecimalFormat;

import java.util.Random;

//随机建立通话的时间, 时间格式：yyyy-MM-dd
public class randomDate {
    public static void main(String[] args) {
        Random random = new Random();
        int duration = random.nextInt(60 * 20) + 1;
        String durationString = new DecimalFormat("0000").format(999);
//        Calendar calendarDate = randomDate("2017-01-01", "2017-10-17");
//        String dateString = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(calendarDate.getTime());

        System.out.println(duration);
    }


}
