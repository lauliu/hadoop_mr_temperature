package temperture;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

//继承Partitioner重写分区函数
public class MyPartition  extends Partitioner<MyKeyYT, Text> {

    //重写分区函数,myKeyYT为map的输出key，text为map的输出value，i为reduce的个数
    @Override
    public int getPartition(MyKeyYT myKeyYT, Text text, int i)
    {
        //按年份进行分区
        return (myKeyYT.getYear()*200)%i;
    }
}
