package TDE4;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.count;

public class Ex01 {
    public static void main(String args[]){
        // Sets ERROR-only logging
        Logger.getLogger("org").setLevel(Level.ERROR);

        // inicializando sessao com duas threads
        SparkSession session = SparkSession.builder().appName("ex01").master("local[2]").getOrCreate();

        // carregando os daods em um dataframe( estrutura mais flexivel que nao requer a codificacao de uma classe)
        DataFrameReader dfr = session.read();

        // dataset<Row> => DATAFRAME!!
//        Dataset<Row> respostas = dfr.option("header", "true").
//                option("inferSchema", "true").
//                csv("in/ommlbd_basico.csv");

        // carregar arquivo
        Dataset<Row> arquivo = session.read().
                option("header", "true").
                option("inferSchema", "true").csv("in/ommlbd_basico.csv");

        /*1)O número de clientes por orientação sexual;*/

        // visualizar o schema
        System.out.println("SCHEMA");
        arquivo.printSchema();
        System.out.println("\n\n\n\n\n\n");

//        arquivo.show(50);

        // contando de acordo com a coluna ORIENTACAO_SEXUAL
        // os diferentes valores contidos nela
        arquivo.groupBy("ORIENTACAO_SEXUAL").
            agg(count("*")).show(10);

        session.stop();
    }
}
