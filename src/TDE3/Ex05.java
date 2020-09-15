package TDE3;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.IOException;

public class Ex05 {

    public static void main(String []args) throws IOException {
        // logger
        Logger.getLogger("org").setLevel(Level.ERROR);

        // habilita o uso de n threads
        SparkConf conf = new SparkConf().setAppName("ex05").setMaster("local[*]");

        // cria o contexto da aplicacao
        JavaSparkContext sc = new JavaSparkContext(conf);

        /*5) Média de peso por mercadoria comercializadas no Brasil (como a base de dados está em
            inglês utilize Brazil, com Z), separadas por ano;*/

        // carregando o arquivo
        JavaRDD<String> linhas = sc.textFile("in/transactions.csv");

        // separando um pair RDD para retornar um tupla com pais
        // e uma string contendo a mercadoria, ano e peso
        JavaPairRDD<String, String> pairRDD = linhas.mapToPair(l -> {
            String[] vals = l.split(";");
            //pais
            String pais = vals[0];

            // mercadoria
            String mercadoria = vals[2];

            // ano
            String ano = vals[1];

            // peso
            String peso = vals[6];
            // se peso estiver vazio entao eh trocado por 0
            if(peso.equals("")){
                peso = "0";
            }

            // concatenando mercadoria, ano e peso
            String chave = new String(mercadoria+";"+ano+";"+peso);

            return new Tuple2<>(pais, chave);});

        // filtrando transacoes somente do Brazil
        JavaPairRDD<String, String> Brazil = pairRDD.filter(
                v -> v._1.equals("Brazil"));

        // separando o pair rdd em chave composta(mercadoria e ano) com valor um objeto
        // Ex05Serializable com peso e ocorrencia(1)
        JavaPairRDD<String, Ex05Serializable> filtroBrazil = Brazil.mapToPair(
                v -> {
                    // separando mercadoria, ano e peso
                    String[] vals = v._2.split(";");

                    // mercadoria
                    String mercadoria = vals[0];

                    // ano
                    String ano = vals[1];

                    //peso
                    String peso = vals[2];

                    String chave = mercadoria+"\t"+ano;

                    return new Tuple2<>(chave, new Ex05Serializable(Float.parseFloat(peso), 1.0f));
                });

        // somando a quantidade e o peso
        JavaPairRDD<String, Ex05Serializable> somados = filtroBrazil.reduceByKey(
                (x, y) -> new Ex05Serializable(x.getQnt() + y .getQnt(),
                        x.getPeso() + y.getPeso()));

        // calcular a media peso/quantidade
        JavaPairRDD<String, Double> resultado =
                somados.mapValues(c -> (double)c.getPeso()/c.getQnt());

        // ordenando em ordem decrescente
        JavaPairRDD<String, Double> ordenado = resultado.sortByKey(true);

        // imprimindo resultado
        resultado.take(20).forEach(c->System.out.println(c._1() + "\t" + c._2()));
//        for(Tuple2<String, Double> c: ordenado.collect()){
//            System.out.print(c._1() + "\t");
//            System.out.println(String.format("%.8f", c._2()));
//        }

        // salvando em arquivo
//        ordenado.coalesce(1).saveAsTextFile("output/ex05.txt");
    }
}
