package sql;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.*;

import static org.apache.spark.sql.functions.*;

public class StronglyTypedStackOverflow {

    public static void main(String args[]) {
        // Sets ERROR-only logging
        Logger.getLogger("org").setLevel(Level.ERROR);

        // inicializando sessao com duas threads
        SparkSession session = SparkSession.builder().appName("houseprice").master("local[2]").getOrCreate();

        // carregar dados
        // dataset<row>(dataframe)
        Dataset<Row> dfr = session.read().
                option("header", "true").
                option("inferSchema", "true").
                csv("in/2016-stack-overflow-survey-responses.csv");

        // WITHCOLUMN: Criar uma coluna nova exatamente com o nome do atributo da classe (ageMidPoint)
        // ALIAS
        dfr = dfr.select(col("country"),
                col("age_midpoint").as("ageMidPoint").cast("integer"),
                col("occupation"),
                col("salary_midpoint").as("salaryMidPoint"));

        // converter em dataset<response>
        Dataset<Response> tipado = dfr.as(Encoders.bean(Response.class));// Java Bean

        // verificando o schema do dataset tipado
        tipado.printSchema();

        // dando uma olhada nos dados
        tipado.show(5);

        // filtro(idade < 20)
        tipado.filter(obj -> obj.getAgeMidPoint() != null &&
                obj.getAgeMidPoint() < 20).show();

        // parando a sessão
        session.stop();

    }

}
