package sql;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;

public class HousePriceSQL {

    private static final String PRICE = "Price";
    private static final String PRICE_SQ_FT = "Price SQ Ft";
    private static final String SIZE = "Size";

    public static void main(String args[]){
        // Sets ERROR-only logging
        Logger.getLogger("org").setLevel(Level.ERROR);

        // inicializando sessao com duas threads
        SparkSession session = SparkSession.builder().appName("houseprice").master("local[2]").getOrCreate();

        DataFrameReader dfr = session.read();

        Dataset<Row> respostas = dfr.option("header", "true").
                option("inferSchema", "true").
                csv("in/RealEstate.csv");

        // visualizar o schema
        System.out.println("SCHEMA");
        respostas.printSchema();
        System.out.println("\n\n\n\n\n\n");

        Dataset<Row> resultado = respostas.groupBy("Location").
                agg(max(PRICE), avg(SIZE), avg(PRICE_SQ_FT)).
                orderBy(col("avg(" + PRICE_SQ_FT + ")").desc());

        // salvando resultado em arquivo
        resultado.coalesce(1).write().mode("overwrite").format("csv").option("header", "true").save("output/precos.csv");
        // parando a sessão
        session.stop();

    }
}
