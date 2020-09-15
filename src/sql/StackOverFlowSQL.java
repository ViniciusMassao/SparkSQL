package sql;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.*;

import static org.apache.spark.sql.functions.*;

public class StackOverFlowSQL {

    public static void main(String args[]){
        // Sets ERROR-only logging
        Logger.getLogger("org").setLevel(Level.ERROR);

        // inicializando sessao com duas threads
        SparkSession session = SparkSession.builder().appName("stackoverflow").master("local[2]").getOrCreate();

        // carregando os daods em um dataframe( estrutura mais flexivel que nao requer a codificacao de uma classe)
        DataFrameReader dfr = session.read();

        // dataset<Row> => DATAFRAME!!
        Dataset<Row> respostas = dfr.option("header", "true").
                option("inferSchema", "true").
                csv("in/2016-stack-overflow-survey-responses.csv");

        // visualizar o schema
        System.out.println("SCHEMA");
        respostas.printSchema();
        System.out.println("\n\n\n\n\n\n");

        /*
        // casting(mudanca de tipo)
        // "age_midpoint" => double
        respostas = respostas.withColumn("age_midpoint",
                col("age_midpoint").cast("double"));

        // visualizar o schema
        System.out.println("SCHEMA");
        respostas.printSchema();
        System.out.println("\n\n\n\n\n\n");
         */

        // olhando as X primeiras linhas
        //System.out.println("INICIO DO DATASET");
        //respostas.show(5);

        /*
        // obter colunas especificas => SELECT
        Dataset<Row> colunasEspecificas = respostas.select(col("so_region"),
                col("star_wars_vs_star_trek"));

        colunasEspecificas.show(5);
        */

        //filtrar (WHERE) => FILTER
        // respostas do Brasil
        //respostas.filter(col("country").equalTo("Brazil")).show(5);

        // mostrando apenas as colunas sw vs st do Brasil
        // filtrar e depois fazer o select

        //respostas.filter(col("country").equalTo("Brazil")).select(col("star_wars_vs_star_trek")).show(5);

        // select e depois filtrar
        // CATALYST (otimizador do sparksql)
        //respostas.select(col("star_wars_vs_star_trek")).filter(col("country").equalTo("Brazil")).show(5);

        // filtro por idade (respostas cujo usuario tem menos de 20 anos de idade)
        //respostas.filter(col("age_midpoint").$less(20)).show(5);

        // ordenacao (asc, desc)
        //respostas.orderBy(col("salary_midpoint").desc()).show();

        // salario medio por pais com ordenacao
        /*
        respostas.groupBy("country").
                agg(avg(col("salary_midpoint"))).
                orderBy(col("avg(salary_midpoint)").desc()).show();
        */

        // contagem
        // contar pessoas de acordo com as colunas sw vs st do Brasil
        respostas.groupBy("star_wars_vs_star_trek").
                agg(count("*")).show(50);

        // parando a sessão
        session.stop();

    }

}
