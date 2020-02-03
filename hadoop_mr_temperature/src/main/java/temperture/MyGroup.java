package temperture;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

//分组和排序其实是一样的，排序好以后将相同的值为一组
public class MyGroup  extends WritableComparator {
    //重写构造方法
   public MyGroup()
   {
       super(MyKeyYT.class,true);
   }

   //将排序的那段copy过来，只要对年份进行分组，所以只取年份的那一部分
    public int compare(WritableComparable a, WritableComparable b)
    {
        MyKeyYT myKeyYT1=(MyKeyYT) a;
        MyKeyYT myKeyYT2=(MyKeyYT) b;
        return Integer.compare(myKeyYT1.getYear(),myKeyYT2.getYear()); //判断是否同一组

    }
}
