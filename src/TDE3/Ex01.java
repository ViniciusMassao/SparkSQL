package TDE3;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.IOException;

public class Ex01 {

    public static void main(String[] args) throws IOException {
        // logger
        Logger.getLogger("org").setLevel(Level.ERROR);

        // habilita o uso de n threads
        SparkConf conf = new SparkConf().setAppName("ex01").setMaster("local[*]");

        // cria o contexto da aplicacao
        JavaSparkContext sc = new JavaSparkContext(conf);

        /*1)O número de transações, por mercadoria, envolvendo o Brasil (como a base de dados está
        em inglês utilize Brazil, com Z);*/

        // carregando o arquivo
        JavaRDD<String> linhas = sc.textFile("in/transactions.csv");

        // separando um pair RDD para retornar uma tupla com pais e mercadoria
        JavaPairRDD<String, String> pairRDD = linhas.mapToPair(l -> {
            String[] vals = l.split(";");
            //pais
            String pais = vals[0];

            // mercadoria
            String mercadoria = vals[3];

            return new Tuple2<>(pais, mercadoria);
        });

        // filtrando transacoes somente do Brazil
        JavaPairRDD<String, String> Brazil = pairRDD.filter(
                v -> v._1.equals("Brazil"));

        // separando o pair rdd em chave = mercadoria e quantidade = 1
        JavaPairRDD<String, Integer> filtroBrazil = Brazil.mapToPair(
                v -> {
                    // pegando mercadoria mercadoria
                    String mercadoria = v._2;

                    return new Tuple2<>(mercadoria, 1);
                });

        // somando as ocorrencias das mercadorias
        JavaPairRDD<String, Integer> resultados = filtroBrazil.reduceByKey((x, y) -> x + y);

        resultados.take(20).forEach(c->System.out.println(c._1() + "\t" + c._2()));

        // salvando em arquivo
//        resultados.coalesce(1).saveAsTextFile("output/ex01.txt");
    }
}
