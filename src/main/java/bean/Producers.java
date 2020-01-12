package bean;

import java.io.Serializable;

public class Producers implements Serializable {
    private static final long serialVersionUID = 1780830599152492490L;
    private String uid;//用户名
    private String pid;//商品id
    private String position;//地址
    private String timeStamp;//时间
    private double price;//商品价格

    @Override
    public String toString() {
        return "Producers{" +
                "uid='" + uid + '\'' +
                ", pid='" + pid + '\'' +
                ", position='" + position + '\'' +
                ", timeStamp='" + timeStamp + '\'' +
                ", price=" + price +
                '}';
    }

    public static long getSerialVersionUID() {
        return serialVersionUID;
    }

    public String getUid() {
        return uid;
    }

    public void setUid(String uid) {
        this.uid = uid;
    }

    public String getPid() {
        return pid;
    }

    public void setPid(String pid) {
        this.pid = pid;
    }

    public String getPosition() {
        return position;
    }

    public void setPosition(String position) {
        this.position = position;
    }

    public String getTimeStamp() {
        return timeStamp;
    }

    public void setTimeStamp(String timeStamp) {
        this.timeStamp = timeStamp;
    }

    public double getPrice() {
        return price;
    }

    public void setPrice(double price) {
        this.price = price;
    }

    @Override
    public boolean equals(Object o) {

        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Producers producers = (Producers) o;

        if (Double.compare(producers.price, price) != 0) return false;
        if (uid != null ? !uid.equals(producers.uid) : producers.uid != null) return false;
        if (pid != null ? !pid.equals(producers.pid) : producers.pid != null) return false;
        if (position != null ? !position.equals(producers.position) : producers.position != null) return false;
        return timeStamp != null ? timeStamp.equals(producers.timeStamp) : producers.timeStamp == null;
    }

    @Override
    public int hashCode() {
        int result;
        long temp;
        result = uid != null ? uid.hashCode() : 0;
        result = 31 * result + (pid != null ? pid.hashCode() : 0);
        result = 31 * result + (position != null ? position.hashCode() : 0);
        result = 31 * result + (timeStamp != null ? timeStamp.hashCode() : 0);
        temp = Double.doubleToLongBits(price);
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        return result;
    }

    public Producers() {

    }

    public Producers(String uid, String pid, String position, String timeStamp, double price) {

        this.uid = uid;
        this.pid = pid;
        this.position = position;
        this.timeStamp = timeStamp;
        this.price = price;
    }
}