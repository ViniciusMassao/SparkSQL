package TDE3;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.IOException;

public class Ex07 {
    public static void main(String []args) throws IOException {
        // logger
        Logger.getLogger("org").setLevel(Level.ERROR);

        // habilita o uso de n threads
        SparkConf conf = new SparkConf().setAppName("ex07").setMaster("local[*]");

        // cria o contexto da aplicacao
        JavaSparkContext sc = new JavaSparkContext(conf);

        /*7) Quantidade de transações comerciais de acordo com o fluxo, de acordo com o ano;*/

        // carregando o arquivo
        JavaRDD<String> linhas = sc.textFile("in/transactions.csv");

        JavaPairRDD<String, Integer> pairRDD = linhas.mapToPair(l -> {
            // separando
            String[] vals = l.split(";");

            // fluxo
            String fluxo = vals[4];

            // ano
            String ano = vals[1];

            String chave = ano + "\t" + fluxo;

            return new Tuple2<>(chave, 1);
        });

        JavaPairRDD<String, Integer> contagem = pairRDD.reduceByKey((x, y) -> x+y);

        JavaPairRDD<String, Integer> resultOrdenado = contagem.sortByKey(true);

        resultOrdenado.take(5).forEach(c->System.out.println(c._1()+" "+c._2()));

    }
}
