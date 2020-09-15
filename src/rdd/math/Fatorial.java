package rdd.math;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.math.BigInteger;
import java.util.ArrayList;

public class Fatorial {

    public static void main (String args[]){

        // Sets ERROR-only logging
        Logger.getLogger("org").setLevel(Level.ERROR);
        // habilita o uso de 2 threads
        SparkConf conf = new SparkConf().setAppName("factorial").setMaster("local[*]");
        // cria o contexto da aplicacao
        JavaSparkContext sc = new JavaSparkContext(conf);

        // numero para calcular o fatorial
        long n = 5;

        // criar um rdd com todos os valores de n a 1
        ArrayList<Long> nums = new ArrayList<Long>();
        while (n != 1){
            nums.add(n);
            n--;
        }

        JavaRDD<Long> rdd = sc.parallelize(nums);

        // reduce (para multiplayer todos os valores)
        Long fatorial = rdd.reduce((x,y)->x*y);
        System.out.println(fatorial);

    }
}
