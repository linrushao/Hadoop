package com.gzgs.mr.A13_RedueceJoin;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;

public class ReduceJoinReducer extends Reducer<Text, OrderPdBean, OrderPdBean, NullWritable> {

    private List<OrderPdBean> orders = new ArrayList<>();//存储一组数据中所有的order数据
    private OrderPdBean pdBean = new OrderPdBean();//存储一组数据中的pd数据

    @Override
    protected void reduce(Text key, Iterable<OrderPdBean> values, Context context) throws IOException, InterruptedException {
        //迭代该组的数据，将来自不同文件kv分别存储
        for (OrderPdBean bean : values) {
            //判断来自于那个文件
            if (bean.getTitle().equals("order")) {
                //来自于order.txt的数据
                try {
                    OrderPdBean currentPdBean = new OrderPdBean();
                    BeanUtils.copyProperties(currentPdBean,bean);
                    orders.add(currentPdBean);
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                } catch (InvocationTargetException e) {
                    e.printStackTrace();
                }
            } else {
                //来自于pd.txt的数据
                try {
                    BeanUtils.copyProperties(pdBean,bean);
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                } catch (InvocationTargetException e) {
                    e.printStackTrace();
                }
            }

            //迭代orders集合中的数据，进行Join操作，并写出
            for(OrderPdBean order:orders){
                order.setPname(pdBean.getPname());
                //写出
                context.write(order, NullWritable.get());
            }

            //清空orders
            orders.clear();
        }
    }
}
