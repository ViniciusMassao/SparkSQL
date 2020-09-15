package rdd.airport;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.Map;


public class AirportUSA {

    // a regular expression which matches commas but not commas within double quotations
    public static final String COMMA_DELIMITER = ",(?=([^\"]*\"[^\"]*\")*[^\"]*$)";

    public static void main(String args[]){
        Logger.getLogger("org").setLevel(Level.ERROR);
        // habilita o uso de 2 threads
        SparkConf conf = new SparkConf().setAppName("airport").setMaster("local[1]");
        // cria o contexto da aplicacao
        JavaSparkContext sc = new JavaSparkContext(conf);

        // carregando o arquivo
        JavaRDD<String> airposts = sc.textFile("in/airports.text");

        /*// quebrar cada linha em palavras
        JavaRDD<String> filtro = linhas.filter(i -> i.contains("United States"));

        filtro.foreach(estado -> System.out.println(estado));*/

        // filter
        int ix_pais = 3;
        airposts.filter(line ->
                line.split(COMMA_DELIMITER)[ix_pais].equals("\"United States\""));

        // map para obter apenas o nome do aeroporto
        int ix_nome_aeroporto = 1;
        JavaRDD<String> apenasNomes = airposts.map(line ->
                line.split(COMMA_DELIMITER)[ix_nome_aeroporto]);

        // removendo potenciais duplicados
        JavaRDD<String> nomesDistintos = apenasNomes.distinct();

        // salvar em arquivo
        airposts.saveAsTextFile("output/airpostUSA.text");

        //salvar em arquivo o rdd apenas com os nomes dos aeroportos
        apenasNomes.saveAsTextFile("output/nomes-aeroprotos.text");

        //salvar em arquivo o rdd com os nomes disitintos dos aeroportos
        nomesDistintos.saveAsTextFile("output/nomesDistinct-aeroprotos.text");
    }
}
