package edu.gzgs.linrushao.coreModel;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

/**
 * @Author linrushao
 * @Date 2023-06-05
 */
public class Contant {

    //用户数据
    public static final String USERS= "/input/data/users.dat";
    //电影数据
    public static final String MOVIES= "/input/data/movies.dat";
    //电影评分
    public static final String RATINGS= "/input/data/ratings.dat";
    //ratings.dat数据和users.dat数据连接【路径】
    public static final String USER_AND_RATING_PATH = "/output/user_gender_prediction/user_and_rating";
    //ratings.dat数据和users.dat数据连接【结果】
    public static final String USER_AND_RATING_RESULT = "/output/user_gender_prediction/user_and_rating/part-m-00000";
    //movies.dat数据和USER_AND_RATING连接结果
    public static final String USER_AND_RATING_MOVIE_PATH = "/output/user_gender_prediction/user_and_rating_genres";
    //用户对所有电影类型的访问次数【性别转换、电影类别统计】路径
    public static final String MOVIES_GENRES_COUNT_PATH = "/output/user_gender_prediction/movie_genres_count_data";
    //用户对所有电影类型的访问次数【性别转换、电影类别统计】结果
    public static final String MOVIES_GENRES_COUNT_RESULT = "/output/user_gender_prediction/movie_genres_count_data/part-r-00000";
    //训练数据路径
    public static final String TRAIN_PATH ="/output/user_gender_prediction/movie_train_data";
    //测试数据路径
    public static final String TEST_PATH="/output/user_gender_prediction/movie_test_data";
    //knn模型输出路径
    public static final String PREDICT_PATH = "/output/user_gender_prediction/knn_output";
    //读取预测结果
    public static final String PREDICT_RESULT = "/output/user_gender_prediction/knn_output/part-r-00000";


}
