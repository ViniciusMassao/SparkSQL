package pairrdd.aggregation;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Function2;
import scala.Tuple2;

import java.io.Serializable;
import java.util.Map;

public class RealEstateReduceAndSortByKey {

    public static void main(String args[]){
        // logger
        Logger.getLogger("org").setLevel(Level.ERROR);
        // habilita o uso de n threads
        SparkConf conf = new SparkConf().setAppName("realEstate").setMaster("local[*]");
        // cria o contexto da aplicacao
        JavaSparkContext sc = new JavaSparkContext(conf);

        //valor medio das casas de acordo com o numero de quartos

        // carregando o arquivo
        JavaRDD<String> linhas = sc.textFile("in/RealEstate.csv");

        // filter para remover o cabecalho
        linhas = linhas.filter(l -> !l.startsWith("MLS"));


        // media = soma(valores) / ocorrencia
        // para cada casa, gerar:
        // <chave, valor>
        // sendo que a chave eh o numero de quartos
        // valor vai ser composto (preco, ocorrencia = 1)


        // criando pair RDD com chave e valor
        JavaPairRDD<Integer, AvgCount> pairRDD = linhas.mapToPair(l -> {
            // quebrando a linha original em campos
            String[] vals = l.split(",");
            // preco (vals[2])
            float preco = Float.parseFloat(vals[2]);
            // numero de quartos (vals[3])
            int numQuartos = Integer.parseInt(vals[3]);

            return new Tuple2<>(numQuartos, new AvgCount(1, preco));
        });

        // somar os precos e ocorrencias para cada numero de quartos
        JavaPairRDD<Integer, AvgCount> somados =
                pairRDD.reduceByKey((x, y) ->
                new AvgCount(x.getQtd() + y.getQtd(),
                        x.getPreco() + y.getPreco()));

        // somados.foreach(v -> System.out.println(v._1() + "\t" + v._2()));

        // calcular a media por numero de quartos
        JavaPairRDD<Integer, Double> resultado =
                somados.mapValues(c -> c.getPreco() / c.getQtd());

        //resultado.foreach(v -> System.out.println(v._1() + "\t" + v._2()));

        JavaPairRDD<Integer, Double> ordenados = resultado.sortByKey((true));
        for(Tuple2<Integer, Double> c: ordenados.collect()){
            System.out.println(c._1() + "\t" + c._2());
        }

    }
}
