package TDE4;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.count;

public class Ex02 {
    public static void main(String args[]){
        // Sets ERROR-only logging
        Logger.getLogger("org").setLevel(Level.ERROR);

        // inicializando sessao com duas threads
        SparkSession session = SparkSession.builder().appName("ex02").master("local[2]").getOrCreate();

        // carregando os daods em um dataframe( estrutura mais flexivel que nao requer a codificacao de uma classe)
        DataFrameReader dfr = session.read();

        // carregar arquivo
        Dataset<Row> arquivo = session.read().
                option("header", "true").
                option("inferSchema", "true").csv("in/ommlbd_basico.csv");

        /*2)O número máximo e mínimo de e-mails cadastrados em toda a base de dados;*/

        // visualizar o schema
        System.out.println("SCHEMA");
        arquivo.printSchema();
        System.out.println("\n\n\n\n\n\n");

        // max
        arquivo.groupBy("QTDEMAIL").
                agg(count("*")).
                orderBy(count("*").desc()).show(1);
        // min
        arquivo.groupBy("QTDEMAIL").
                agg(count("*")).
                orderBy(count("*").asc()).show(1);

        session.stop();
    }
}
