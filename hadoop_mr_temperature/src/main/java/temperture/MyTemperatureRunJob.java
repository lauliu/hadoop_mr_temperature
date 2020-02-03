package temperture;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class MyTemperatureRunJob  {
    public static SimpleDateFormat sdf=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    static class MyTemperatureMapper extends Mapper<LongWritable, Text,MyKeyYT,Text>
    {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String []ss =line.split("\t");

            if(ss.length==2)
            {
                try
                {
                    Date mydate=sdf.parse(ss[0]); //获取数组的第一个元素

                    //拿年份
                    Calendar myCalendar=Calendar.getInstance();
                    myCalendar.setTime(mydate);
                    int year =myCalendar.get(1);

                    String myhot = ss[1].substring(0,ss[1].indexOf("℃"));

                    //创建自定的MyKeyYT对象
                    MyKeyYT myKeyYT=new MyKeyYT();
                    myKeyYT.setYear(year);
                    myKeyYT.setHot(Integer.parseInt(myhot));

                    context.write(myKeyYT,value);

                } catch (Exception e) {
                    e.printStackTrace();
                }

            }
        }
    }

    static class MyTemperatureReducer extends Reducer<MyKeyYT,Text,MyKeyYT,Text>
    {
        @Override
        protected void reduce(MyKeyYT key, Iterable<Text> values, Context context) throws IOException, InterruptedException
        {
            for(Text v:values)
            {
                context.write(key,v);
            }
        }
    }

    public static void main(String[] args)
    {
        //获取环境变量,设置提交该Job的mapred.job.tracker
        Configuration conf =new Configuration();

        //配置mapreduce.job.tracker，
        //和集群mapred-site.xml里面的属性 保持一致即可，
        //此句也可以不写，直接省略。
        // conf.set("mapreduce.job.tracker","dw-cluster-master:9001");

        try
        {
            //mapreduce输出结果会自动创建folder，
            //但是如果指定的输出target folder如果已存在，是会报错的，
            //这段是做容错，可以让程序rerun
            Path outputPath= new Path(args[2]);
            FileSystem fileSystem =FileSystem.get(conf);
            if(fileSystem.exists(outputPath)){
                fileSystem.delete(outputPath,true);
                System.out.println("outputPath is exist,but has deleted!");
            }

            Job myjob= Job.getInstance(conf);
            myjob.setJarByClass(MyTemperatureRunJob.class);//指定调用的WcJobRun Class打成Jar再跑
            myjob.setMapperClass(MyTemperatureMapper.class);//指定Map类
            myjob.setReducerClass(MyTemperatureReducer.class);//指定Reduce类
            myjob.setMapOutputKeyClass(MyKeyYT.class);//指定Map的输出key类型
            myjob.setMapOutputValueClass(Text.class);//指定Map输出的value的类型

            myjob.setNumReduceTasks(7);//指定reduce的个数，有7个年份
            myjob.setPartitionerClass(MyPartition.class); //引用自定义的partition
            myjob.setSortComparatorClass(MySortTemp.class); //引用自定义的sort排序
            myjob.setGroupingComparatorClass(MyGroup.class); //引用自定义的分组


            //为什么用args[1]，因为args[0]第一个参数留给main方法所在的Class
            FileInputFormat.addInputPath(myjob,new Path(args[1]));//指定整个Job的输入文件路径，args[1]表示调用Jar包时，紧跟Jar包的第二个参数
            //FileInputFormat.addInputPath(myjob,new Path("/tmp/wcinput/wordcount.xt"));
//指定整个Job的输出文件路径，args[2]表示调用Jar包时，紧跟Jar包的第三个参数
            FileOutputFormat.setOutputPath(myjob,new Path(args[2]));
            //FileOutputFormat.setOutputPath(myjob,new Path("/tmp/wcoutput"));
            System.exit(myjob.waitForCompletion(true)?0:1);//等待Job完成，正确完成则退出
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }



    }



}
