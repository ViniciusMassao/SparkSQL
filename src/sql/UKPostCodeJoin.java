package sql;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;

public class UKPostCodeJoin {

    public static void main(String args[]) {
        // Sets ERROR-only logging
        Logger.getLogger("org").setLevel(Level.ERROR);

        // inicializando sessao com duas threads
        SparkSession session = SparkSession.builder().appName("ukpostcode").master("local[2]").getOrCreate();

        // carregar arquivo
        Dataset<Row> makerpace = session.read().
                option("header", "true").
                option("inferSchema", "true").csv("in/uk-makerspaces-identifiable-data.csv");

        Dataset<Row> postcode = session.read().
                option("header", "true").
                option("inferSchema", "true").csv("in/uk-postcode.csv").
                withColumn("Postcode", concat_ws("", col("Postcode"), lit(" ")));
        //fazendo um append de espaco no final de cada string original

        //makerpace.show(5);
        //postcode.show(5);

        // fazendo join
        Dataset<Row> joined = makerpace.join(postcode,
                makerpace.col("Postcode").startsWith(postcode.col("Postcode")),
                "inner");

        // olhando resultado
        joined.show(10);

        // parando a sessão
        session.stop();

    }
}