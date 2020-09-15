package TDE3;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.IOException;

public class Ex04 {
    public static void main(String []args) throws IOException {
        // logger
        Logger.getLogger("org").setLevel(Level.ERROR);

        // habilita o uso de n threads
        SparkConf conf = new SparkConf().setAppName("ex04").setMaster("local[*]");

        // cria o contexto da aplicacao
        JavaSparkContext sc = new JavaSparkContext(conf);

        /*4) Média de peso por mercadoria, separadas de acordo com o ano;*/

        // carregando o arquivo
        JavaRDD<String> linhas = sc.textFile("in/transactions.csv");

        // fazendo map que tenha como chave composta a mercadoria(codigo) e ano
        // e como valor um objeto Ex04Serializable que contenha peso e quantidade das ocorrencias
        JavaPairRDD<String, Ex04Serializable> pairRDD = linhas.mapToPair(l -> {
            String[] vals = l.split(";");

            // mercadoria - codigo da mercadoria
            String mercadoria = vals[2];

            // peso
            String peso = vals[6];
            // se peso estiver vazio entao eh trocado por 0
            if (peso.equals("")) {
                peso = "0";
            }

            // ano
            String ano = vals[1];

            // chave composta com mercadoria e ano
            String chave = mercadoria + " " + ano;
            return new Tuple2<>(chave, new Ex04Serializable( 1.0f, Float.parseFloat(peso)));
        });

        // somando peso e quantidade
        JavaPairRDD<String, Ex04Serializable> somandoPairRDD = pairRDD.reduceByKey((x, y) ->
                new Ex04Serializable(x.getQnt() + y.getQnt(),
                x.getPeso() + y.getPeso()));

        // pegando a media de peso / quantidade por chave
        JavaPairRDD<String, Double> mediaPairRDD = somandoPairRDD.mapValues(c -> (double) c.getPeso() / c.getQnt());

        // ordenando em ordem ascendente
        JavaPairRDD<String, Double> ordenados = mediaPairRDD.sortByKey(true);

        // mostrando os 5 primeiro resultados
        ordenados.take(5).forEach(c->System.out.println(c._1() + " " + c._2()));

    }
}
