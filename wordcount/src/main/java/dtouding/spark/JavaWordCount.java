package dtouding.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

public class JavaWordCount {

    public static void main(String[] args) {
        //创建Spark配置，设置应用名
        //SparkConf conf = new SparkConf().setAppName("JavaWordCount"); //提交到集群
        SparkConf conf = new SparkConf().setAppName("JavaWordCount").setMaster("local[2]"); //本地执行

        //创建Spark执行的入口
        JavaSparkContext sc = new JavaSparkContext(conf);
        //读取文件
        JavaRDD<String> lines = sc.textFile(args[0]);
        //切分压平
        JavaRDD<String> words = lines.flatMap(line -> Arrays.asList(line.split(" ")).iterator());
        //将单词和1组成元组
        JavaPairRDD<String, Integer> tuples = words.mapToPair(word -> new Tuple2<>(word, 1));
        //按word聚合
        JavaPairRDD<String, Integer> reducedTuples = tuples.reduceByKey((m, n) -> m + n);
        //交换word和count的位置，方便按照key排序
        JavaPairRDD<Integer, String> swapTuples = reducedTuples.mapToPair(tp -> tp.swap());
        //排序
        JavaPairRDD<Integer, String> sortedTuples = swapTuples.sortByKey(false);
        //再次将word和count交换回去
        JavaPairRDD<String, Integer> result = sortedTuples.mapToPair(tp -> tp.swap());
        //保存结果到hdfs
        result.saveAsTextFile(args[1]);
        //释放资源
        sc.stop();
    }
}
