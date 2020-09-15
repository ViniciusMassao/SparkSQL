package rdd.math;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.Map;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class HelloWorld {

    public static void main(String args[]){
        // setando o nivel de LOG
        Logger.getLogger("org").setLevel(Level.ERROR);

        // habilita o uso de n threads
        SparkConf conf = new SparkConf().setAppName("wordCount").setMaster("local[*]");

        // cria o contexto da aplicacao
        JavaSparkContext sc = new JavaSparkContext(conf);

        // carregando o arquivo
        JavaRDD<String> rdd = sc.textFile("in/word_count.text");

        // quebrar cada linha em palavras
        JavaRDD<String> palavras = rdd.flatMap(line -> Arrays.asList(line.split(" ")).iterator());

        // contar as palavras
        Map<String, Long> resultado = palavras.countByValue();

        // apresentar resultado
        for(Map.Entry<String, Long> v: resultado.entrySet())
            System.out.println(v.getKey() + "\t" + v.getValue());

    }
}
