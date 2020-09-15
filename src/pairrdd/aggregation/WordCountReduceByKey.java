package pairrdd.aggregation;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Map;

public class WordCountReduceByKey {

    public static void main(String args[]){
        // logger
        Logger.getLogger("org").setLevel(Level.ERROR);
        // habilita o uso de n threads
        SparkConf conf = new SparkConf().setAppName("wordCount").setMaster("local[*]");
        // cria o contexto da aplicacao
        JavaSparkContext sc = new JavaSparkContext(conf);

        // countByValue() -> Map<> (todos os dados vao ate o driver)
        JavaRDD<String> rdd = sc.textFile("in/word_count.text");

        // Map(1:1) para cada elemento do rdd original, eu vou ter
        // 1 elemento no rdd de saida

        // FlatMap(1:n) oara cada elemento do rdd original,
        // eu posso ter varios valores no rdd de saida

        // quebrando em palavras
        JavaRDD<String> palavras =
                rdd.flatMap(l ->
                        Arrays.asList(l.split(" ")).iterator());

        // gerando(palavra, 1)
        JavaPairRDD<String, Integer> palavrasUnitarias = palavras.mapToPair(p -> new Tuple2<>(p, 1));

        /*
        // groupByKey
        JavaPairRDD<String, Iterable<Integer>> grouped = palavrasUnitarias.groupByKey();

        grouped.coalesce(1).saveAsTextFile("output/groupbykey.txt");


        // somando as ocorrencias por palavras
        JavaPairRDD<String, Integer> resultados = palavrasUnitarias.reduceByKey((x, y) -> x + y);
        // Multi-thread
        resultados.foreach(v -> System.out.println(v._1()  +"\t" + v._2()));

        // convertendo o resultado em um map para print
        Map<String, Integer> rs = resultados.collectAsMap();
        // Single-thread
        for(Map.Entry<String, Integer> m: rs.entrySet()){
            System.out.println(m.getKey()  +"\t" + m.getValue());
        }
        */

        //exercicio
        JavaPairRDD<String, Integer> resultados = palavrasUnitarias.reduceByKey((x, y) -> x + y);

        // inverter chave com valor(p, ocorrencia) -> (ocorrencia, palavra)
        JavaPairRDD<Integer, String> invertido = resultados.mapToPair(p -> new Tuple2<>(p._2(), p._1()));

        // ordenando por ocorrencia
        JavaPairRDD<Integer, String> ordenado = invertido.sortByKey(false);

        //desinverter
        resultados = ordenado.mapToPair(p -> new Tuple2<>(p._2(), p._1()));
        for(Tuple2<String, Integer> m: resultados.collect()){
            System.out.println(m._1()  +"\t" + m._2());
        }
    }
}
