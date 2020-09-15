package TDE4;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.*;
import org.apache.spark.sql.catalyst.expressions.Encode;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.count;

public class Ex03 {
    public static void main(String args[]) {
        // Sets ERROR-only logging
        Logger.getLogger("org").setLevel(Level.ERROR);

        // inicializando sessao com duas threads
        SparkSession session = SparkSession.builder().appName("ex03").master("local[2]").getOrCreate();

        // carregando os daods em um dataframe( estrutura mais flexivel que nao requer a codificacao de uma classe)
        DataFrameReader dfr = session.read();

        // dataset<Row> => DATAFRAME!!
//        Dataset<Row> respostas = dfr.option("header", "true").
//                option("inferSchema", "true").
//                csv("in/ommlbd_basico.csv");

        // carregar arquivo
        Dataset<Row> arquivo = session.read().
                option("header", "true").
                option("inferSchema", "true").csv("in/ommlbd_renda.csv");

        /*3)O número de propostas cujo cliente possui estimativa de renda seja superior a R$
            10.000,00 (dez mil reais);*/

        // visualizar o schema
        System.out.println("SCHEMA");
        arquivo.printSchema();
        System.out.println("\n\n\n\n\n\n");

        arquivo = arquivo.select(col("HS_CPF").as("hs_cpf"),
                col("ESTIMATIVARENDA").as("estimativa_renda"));

        Dataset<Ex03Serializable>tipado = arquivo.as(Encoders.bean(Ex03Serializable.class));

        tipado.filter(obj-> obj.getEstimativa_renda()>10000);

        tipado.groupBy("HS_CPF").
                agg(count("*")).
                orderBy(count("*")).
                show();

    }
}
