package temperture;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

//重写排序
public class MySortTemp extends WritableComparator
{
    //重写一下构造方法
    public  MySortTemp()
    {
        //将Map的输出进行比较
        super(MyKeyYT.class,true);
    }

    public int compare(WritableComparable a, WritableComparable b)
    {
        MyKeyYT myKeyYT1=(MyKeyYT) a;
        MyKeyYT myKeyYT2=(MyKeyYT) b;
        int myresult = Integer.compare(myKeyYT1.getYear(),myKeyYT2.getYear()); //升序排序
        if(myresult!=0)
            return myresult;
        return -Integer.compare(myKeyYT1.getHot(),myKeyYT2.getHot()); //降序排序
    }
}
