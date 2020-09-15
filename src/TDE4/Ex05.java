package TDE4;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.*;

import static org.apache.spark.sql.functions.*;

public class Ex05 {

    public static void main(String args[]) {
        // Sets ERROR-only logging
        Logger.getLogger("org").setLevel(Level.ERROR);

        // inicializando sessao com duas threads
        SparkSession session = SparkSession.builder().appName("Ex05").master("local[2]").getOrCreate();

        // carregando os daods em um dataframe( estrutura mais flexivel que nao requer a codificacao de uma classe)
        DataFrameReader dfr = session.read();

        //5. O percentual de propostas de crédito cujo cliente possui um funcionário público em casa (use a coluna FUNCIONARIOPUBLICOCASA);

        // carregar arquivo
        Dataset<Row> basico = session.read().
                option("header", "true").
                option("inferSchema", "true").csv("in/ommlbd_basico.csv");

        Dataset<Row> familiar = session.read().
                option("header", "true").
                option("inferSchema", "true").csv("in/ommlbd_familiar.csv").
                withColumn("HS_CPF", concat_ws("", col("HS_CPF"), lit(" ")));

        Dataset<Row> filtrado = familiar.filter(col("FUNCIONARIOPUBLICOCASA").notEqual("-9999"));

        // visualizar o schema
        System.out.println("SCHEMA");
        familiar.printSchema();
        System.out.println("\n\n\n\n\n\n");

        //filtrando por tendo funcionario publico em casa
        filtrado.agg(sum("FUNCIONARIOPUBLICOCASA").$div(count("FUNCIONARIOPUBLICOCASA")).multiply(100)).show();

        // parando a sessão
        session.stop();
    }
}
