package TDE4;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.*;

import static org.apache.spark.sql.functions.*;

public class Ex06 {

    public static void main(String args[]) {
        // Sets ERROR-only logging
        Logger.getLogger("org").setLevel(Level.ERROR);

        // inicializando sessao com duas threads
        SparkSession session = SparkSession.builder().appName("Ex06").master("local[2]").getOrCreate();

        // carregando os daods em um dataframe( estrutura mais flexivel que nao requer a codificacao de uma classe)
        DataFrameReader dfr = session.read();

        //6. O percentual de propostas de crédito cujo cliente vive em uma cidade com IDH em cada
        //uma das faixas: 0 a 10, 10 a 20, 20 a 30, 30 a 40, 40 a 50, 50 a 60, 60 a 70, 70 a 80, 80 a
        //90 e 90 a 100;

        // carregar arquivo
        Dataset<Row> basico = session.read().
                option("header", "true").
                option("inferSchema", "true").csv("in/ommlbd_basico.csv").
                withColumn("HS_CPF", concat_ws("", col("HS_CPF"), lit(" ")));

        Dataset<Row> regional = session.read().
                option("header", "true").
                option("inferSchema", "true").csv("in/ommlbd_regional.csv").
                withColumn("HS_CPF", concat_ws("", col("HS_CPF"), lit(" ")));

        //retirando dados inconsistentes
        Dataset<Row> filtrado = regional.filter(col("IDHMUNICIPIO").notEqual("-9999"));
        Dataset<Row> filtrado_basico = basico.filter(col("DISTCENTROCIDADE").notEqual("-9999"));

        //fazendo join
        Dataset<Row> joined = filtrado_basico.join(filtrado,
                filtrado_basico.col("HS_CPF").startsWith(filtrado.col("HS_CPF")),
                "inner");

        //dividindo a parte de treino
        Dataset<Row> treino = joined.filter(col("SAFRA").equalTo("TREINO"));
        //dividindo a parte de teste
        Dataset<Row> teste = joined.filter(col("SAFRA").equalTo("TESTE"));



        // visualizar o schema
        //System.out.println("SCHEMA");
        //filtrado.printSchema();
        //System.out.println("\n\n\n\n\n\n");



        //filtrando por faixa de IDH
        //resultados para treino
        /*System.out.println("Respostas Treino");
        long max = treino.select(col("IDHMUNICIPIO")).count();
        int j = 10;
        for(int i = 0; i<= 90; i=i+10){
            System.out.println(i +" ate " + j);
            treino.filter(col("IDHMUNICIPIO").between(i, j)).agg(count("*").divide(max).multiply(100)).show();
            j = j+10;
        }*/

        //filtrando por tendo a estimativa de renda maior que R$10.000,00
        //resultados para teste
        long max =teste.select(col("IDHMUNICIPIO")).count();
        int j = 10;
        System.out.println("Respostas Teste");
        for(int i = 0; i<= 90; i=i+10){
            System.out.println(i +" ate " + j);
            teste.filter(col("IDHMUNICIPIO").between(i, j)).agg(count("*").divide(max).multiply(100)).show();
            j = j+10;
        }

        // parando a sessão
        session.stop();
    }
}
