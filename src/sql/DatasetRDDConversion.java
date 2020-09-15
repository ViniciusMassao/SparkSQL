package sql;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;

import static org.apache.spark.sql.functions.col;

public class DatasetRDDConversion {

    public static final String COMMA_DELIMITER = ",(?=([^\"]*\"[^\"]*\")*[^\"]*$)";

    public static void main(String args[]) {
        // Sets ERROR-only logging
        Logger.getLogger("org").setLevel(Level.ERROR);

        SparkConf conf = new SparkConf().setAppName("conversion").setMaster("local[2]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // inicializando sessao com duas threads
        SparkSession session = SparkSession.builder().appName("houseprice").master("local[2]").getOrCreate();

        // carregando dados em um RDD
        JavaRDD<String> rdd = sc.textFile("in/2016-stack-overflow-survey-responses.csv");

        // converter para um RDD <Response>
        JavaRDD<Response> rddResponse = rdd.filter(l -> !l.startsWith(",collector,country,un_subregion,so_region,")).
                    map(l -> {
                        String[] vals = l.split(COMMA_DELIMITER, -1);
                        String country = vals[2];
                        Double ageMidPoint = !vals[6].equals("") ? Double.parseDouble(vals[6]): 0.0;
                        String occupation = vals[9];
                        double salaryMidPoint = !vals[14].equals("") ? Double.parseDouble(vals[14]) : 0.0;
                        return new Response(country, ageMidPoint.intValue(), occupation, salaryMidPoint);
                    });

        // converter esse RDD para Dataset<Response>
        Dataset<Response> dataset = session.createDataset(rddResponse.rdd(), Encoders.bean(Response.class));
        dataset.printSchema();
        dataset.show(5);

        // voltando para um RDD
        JavaRDD<Response> volta = dataset.toJavaRDD();

        volta.foreach(v -> System.out.println(v));


        // parando a sessão
        session.stop();

    }
}
