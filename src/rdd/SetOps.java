package rdd;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Int;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class SetOps {
    public static void main (String args[]){
        Logger.getLogger("org").setLevel(Level.ERROR);
        // habilita o uso de 2 threads
        SparkConf conf = new SparkConf().setAppName("setOps").setMaster("local[2]");
        // cria o contexto da aplicacao
        JavaSparkContext sc = new JavaSparkContext(conf);

        // criando RDDs
        JavaRDD<Integer> rddA = sc.parallelize(Arrays.asList(1,2,3,4,5));
        JavaRDD<Integer> rddB = sc.parallelize(Arrays.asList(1, 3));
        JavaRDD<String> rddC = sc.parallelize(Arrays.asList("A", "B"));

        // union
        JavaRDD<Integer> rddUniao = rddA.union(rddB);
        System.out.println("---------UNIAO---------");
        for(Integer v : rddUniao.collect()){ // collect transforma rdd em lista
            System.out.println(v);
        }
        System.out.println("-----------------------");

        // intersection
        JavaRDD<Integer> rddIntersection = rddA.intersection(rddB);
        System.out.println("---------INTERSECAO---------");
        rddIntersection.foreach(v -> System.out.println(v));
        System.out.println("-----------------------");

        // subtract
        JavaRDD<Integer> rddSubtract = rddA.subtract(rddB);
        System.out.println("---------SUBTRACAO---------");
        rddSubtract.foreach(v -> System.out.println(v));
        System.out.println("-----------------------");

        // cartesian product
        JavaPairRDD<Integer, String> cartesian = rddA.cartesian(rddC);
        List<Tuple2<Integer, String>> listaCartesian = cartesian.collect();
        for(Tuple2<Integer, String> v: listaCartesian){
            System.out.println(v._1 + "\t" + v._2);
        }

        // sample -> transformacao
        JavaRDD<Integer> sample = rddA.sample(false, 0.5);
        System.out.println("---------SAMPLE---------");
        sample.foreach(v -> System.out.println(v));
        System.out.println("------------------------");

        // take -> operacao
        List<Integer> take = rddA.take(2);
        System.out.println("---------TAKE---------");
        for(Integer v: take){
            System.out.println(v);
        }
        System.out.println("----------------------");
    }
}
