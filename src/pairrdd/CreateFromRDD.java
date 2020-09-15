package pairrdd;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;

public class CreateFromRDD {
    public static void main (String args[]){
        // Sets ERROR-only logging
        Logger.getLogger("org").setLevel(Level.ERROR);
        // habilita o uso de n threads
        SparkConf conf = new SparkConf().setAppName("union").setMaster("local[*]");
        // cria o contexto da aplicacao
        JavaSparkContext sc = new JavaSparkContext(conf);


        // criar um RDD
        JavaRDD<String> rdd = sc.parallelize(Arrays.asList("Jean,50", "Pavao,80", "Orlando,20"));

        // convertendo em PairRDD
        JavaPairRDD<String, Integer> pairRDD = rdd.mapToPair(getNomeAno());

        // salvando em arquivo
        pairRDD.saveAsTextFile("output/pairrdd_1.txt");

    }

    private static PairFunction<String, String, Integer> getNomeAno(){
        return pessoa -> {
            String[] vals = pessoa.split(",");
            String nome = vals[0];
            Integer idade = Integer.parseInt(vals[1]);
            return new Tuple2<>(nome, idade);
        };
    }

}
