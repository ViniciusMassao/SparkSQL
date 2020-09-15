package TDE3;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.IOException;

public class Ex02 {

    public static void main(String[] args) throws IOException {
        // logger
        Logger.getLogger("org").setLevel(Level.ERROR);

        // habilita o uso de n threads
        SparkConf conf = new SparkConf().setAppName("ex02").setMaster("local[*]");

        // cria o contexto da aplicacao
        JavaSparkContext sc = new JavaSparkContext(conf);

        /* 2)Quantidade de transações financeiras realizadas por ano;*/

        // carregando o arquivo
        JavaRDD<String> linhas = sc.textFile("in/transactions.csv");

        // separando o pair rdd em chave = ano e quantidade = 1
        JavaPairRDD<String, Integer> pairRDD = linhas.mapToPair(l -> {
            String[] vals = l.split(";");

            // ano
            String ano = vals[1];

            return new Tuple2<>(ano, 1);
        });

        // somando as ocorrencias dos anos
        JavaPairRDD<String, Integer> resultados = pairRDD.reduceByKey((x, y) -> x + y);

        resultados = resultados.sortByKey(true);

        resultados.take(20).forEach(c->System.out.println(c._1() + "\t" + c._2()));

        // salvando em arquivo
//        resultados.coalesce(1).saveAsTextFile("output/ex02.txt");
    }
}
