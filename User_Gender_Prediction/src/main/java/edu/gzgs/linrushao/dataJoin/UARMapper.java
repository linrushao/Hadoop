package edu.gzgs.linrushao.dataJoin;


import edu.gzgs.linrushao.coreModel.Contant;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
/**
 * @Author LinRuShao
 * @Date 2023/5/26 11:25
 */
/*
 * 根据UserID（用户ID）字段连接ratings.dat数据和users.dat数据
 * 连接结果得到一份包含UserID（用户ID），Gender（性别），MovieID（电影ID）的数据
 * */
public class UARMapper extends Mapper<LongWritable, Text,Text, NullWritable> {
    Map<String,String> user_gender = new HashMap<>();
    Text output = new Text();
    /*
     * 读取users.dat的数据,以userid为键，以性别为值，存入HashMap
     * */
    @Override
    protected void setup(Mapper<LongWritable, Text, Text, NullWritable>.Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        FileSystem fs = FileSystem.get(conf);

        FSDataInputStream user = fs.open(new Path(Contant.USERS));
        BufferedReader br = new BufferedReader(new InputStreamReader(user));
        String line="";
        while ((line = br.readLine())!=null){
            String[] values = line.split("::");
            user_gender.put(values[0],values[1]);
        }
        br.close();
    }

    /*
     * 遍历ratings.dat每一条数据，分割出userid，
     * 在HashMap搜索该userid对应的值进行字符串连接，
     * 得到用户id，性别，电影ID
     * */
    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, NullWritable>.Context context) throws IOException, InterruptedException {
        String userId = value.toString().split("::")[0];
        String movieId = value.toString().split("::")[1];
        output.set(userId+"::"+user_gender.get(userId)+"::"+movieId);
        context.write(output,NullWritable.get());
    }
}
