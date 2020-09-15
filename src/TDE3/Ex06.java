package TDE3;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.IOException;

public class Ex06 {

    //private static float min = Float.MIN_VALUE;
    public static void main(String []args) throws IOException {
        // logger
        Logger.getLogger("org").setLevel(Level.ERROR);

        // habilita o uso de n threads
        SparkConf conf = new SparkConf().setAppName("ex06").setMaster("local[*]");

        // cria o contexto da aplicacao
        JavaSparkContext sc = new JavaSparkContext(conf);

        /*6) Mercadoria com o maior preço por unidade de peso;*/

        // carregando o arquivo
        JavaRDD<String> linhas = sc.textFile("in/transactions.csv");

        JavaPairRDD<String, Ex06Serializable> pairRDD = linhas.mapToPair(l -> {
            // separando
            String[] vals = l.split(";");

            // unidade de medida
            String unidade = vals[7];

            // mercadoria
            String mercadoria = vals[3];

            // preco
            float preco = Float.parseFloat(vals[5]);

            return new Tuple2<>(unidade,new Ex06Serializable(mercadoria,preco,unidade));
        });

        // usando reducebykey para encontrar o maior preco,
        // caso x tenha um valor maior(if), entao sera colocado x.getPreco como o valor da chave
        // caso y tenha um valor maior(else), entao sera colocado y.getPreco como o valor da chave
        JavaPairRDD<String, Ex06Serializable> reduced = pairRDD.reduceByKey((x, y) ->
                x.getPreco() >= y.getPreco() ? new Ex06Serializable(x.getMercadoria(), x.getPreco(), x.getUnidade())
                        : new Ex06Serializable(y.getMercadoria(), y.getPreco(), y.getUnidade()));

        reduced.take(13).forEach(c->System.out.println(c._1()+"\t"+c._2()));
    }
}