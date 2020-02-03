package temperture;

import org.apache.hadoop.io.WritableComparable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

//自定义的类型MyKeyYT不能直接使用于org.apache.hadoop.io，需要继承接口WritableComparable，里面接一个泛型，就是他自己
//继承接口WritableComparable需要重写readFields，writecompareTo
public class MyKeyYT implements WritableComparable<MyKeyYT>
{
    private int year;
    private int hot;

    public void setYear(int year) {
        this.year = year;
    }

    public int getYear() {
        return year;
    }

    public void setHot(int hot) {
        this.hot = hot;
    }

    public int getHot() {
        return hot;
    }

    //hadoop使用的是rpc协议，里面的数据是二进制流，转化成对象需要反系列化readFields
    public void readFields(DataInput dataInput) throws IOException
    {
        this.year=dataInput.readInt();
        this.hot=dataInput.readInt();

    }

    //序列化，将对象中的year和hot序列化成二进制流
    public void write(DataOutput dataOutput) throws IOException
    {
        dataOutput.writeInt(year);
        dataOutput.writeInt(hot);
    }

    //比较，将传入的MyKeyYT o对象与当前对象进行比较，确定是否为同一个key
    public int compareTo(MyKeyYT o)
    {
        int myresult = Integer.compare(year,o.getYear());
        if(myresult!=0)
            return myresult;
        return Integer.compare(hot,o.getHot());
    }

    //重写toString
    @Override
    public String toString() {
        return year+"\t"+hot;
    }

    //重写hashCode，随便写，只要和之前的不一致就可
    @Override
    public int hashCode() {
        return new Integer(year+hot).hashCode();
    }
}
