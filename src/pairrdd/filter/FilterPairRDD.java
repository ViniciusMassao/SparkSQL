package pairrdd.filter;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import javax.xml.crypto.dsig.keyinfo.KeyValue;

public class FilterPairRDD {

    public static final String COMMA_DELIMITER = ",(?=([^\"]*\"[^\"]*\")*[^\"]*$)";

    public static void main (String args[]) {
        // Sets ERROR-only logging
        Logger.getLogger("org").setLevel(Level.ERROR);
        // habilita o uso de n threads
        SparkConf conf = new SparkConf().setAppName("filterPairRDD").setMaster("local[*]");
        // cria o contexto da aplicacao
        JavaSparkContext sc = new JavaSparkContext(conf);

        // carregando dados dos aeroportos
        JavaRDD<String> rdd = sc.textFile("in/airports.text");

        // gerar pair rdd com chave sendo o nome do areoporto
        // valor vai ser o pais do aeroporto

        JavaPairRDD<String, String> pairRDD = rdd.mapToPair(l -> {
            String[] vals = l.split(COMMA_DELIMITER);
            return new Tuple2<>(vals[1], vals[3]); // indice 1 (nome), indice 3 (pais)
        });

        // aeroportos no Brasil
        JavaPairRDD<String, String> br = pairRDD.filter(v -> v._2.equals("\\\"Brazil\""));

        // convertendo os paises para caixa alta
        JavaPairRDD<String, String> brUpperCase = br.mapValues(v -> v.toUpperCase());

        // salvando em arquivo
        brUpperCase.coalesce(1).saveAsTextFile("output/aero_brasil.txt");
    }
}
