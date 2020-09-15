package TDESQL;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.*;

import static org.apache.spark.sql.functions.*;

public class Ex07 {

    public static void main(String args[]) {
        // Sets ERROR-only logging
        Logger.getLogger("org").setLevel(Level.ERROR);

        // inicializando sessao com duas threads
        SparkSession session = SparkSession.builder().appName("Ex07").master("local[2]").getOrCreate();

        // carregando os daods em um dataframe( estrutura mais flexivel que nao requer a codificacao de uma classe)
        DataFrameReader dfr = session.read();

        //3. O número de propostas cujo cliente possui estimativa de renda seja superior a R$10.000,00 (dez mil reais);

        // carregar arquivo

        Dataset<Row> basico = session.read().
                option("header", "true").
                option("inferSchema", "true").csv("in/ommlbd_basico.csv").
                withColumn("HS_CPF", concat_ws("", col("HS_CPF"), lit(" ")));

        Dataset<Row> renda = session.read().
                option("header", "true").
                option("inferSchema", "true").csv("in/ommlbd_renda.csv").
                withColumn("HS_CPF", concat_ws("", col("HS_CPF"), lit(" ")));

        Dataset<Row> distzonarisco = basico.filter(col("DISTZONARISCO").$less(5000));

        Dataset<Row> joined = distzonarisco.join(renda,
              distzonarisco.col("HS_CPF").startsWith(renda.col("HS_CPF")),
               "inner");


        //Dataset<Row> distzonarisco = joined.filter(col("DISTZONARISCO").$less(5000));
        joined.filter(col("ESTIMATIVARENDA").$greater(7000)).agg(count("*")).show(50);


        // visualizar o schema
        System.out.println("SCHEMA");
        renda.printSchema();
        System.out.println("\n\n\n\n\n\n");


        // parando a sessão
        session.stop();
    }
}
