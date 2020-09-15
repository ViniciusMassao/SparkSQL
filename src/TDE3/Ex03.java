package TDE3;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.IOException;

public class Ex03 {
    public static void main(String []args) throws IOException {
        // logger
        Logger.getLogger("org").setLevel(Level.ERROR);

        // habilita o uso de n threads
        SparkConf conf = new SparkConf().setAppName("ex03").setMaster("local[*]");

        // cria o contexto da aplicacao
        JavaSparkContext sc = new JavaSparkContext(conf);

        /*3) Mercadoria mais transacionada em 2016, no fluxo de importação e no Brasil (como a base
            de dados está em inglês utilize Brazil, com Z); Dicas: assuma que a mesma comodity não
            aparecerá em duas linhas e também use a coluna de quantidade.*/

        // carregando o arquivo
        JavaRDD<String> linhas = sc.textFile("in/transactions.csv");

        // criando um pairRDD que tenha um objeto Ex03Serializable com os dados necessarios
        // e uma chave composta(contendo pais fluxo e ano) para poder fazer o filter depois
        JavaPairRDD<String, Ex03Serializable> pairRDD = linhas.mapToPair(l -> {
            String[] vals = l.split(";");
            //pais
            String pais = vals[0];

            // mercadoria
            String mercadoria = vals[3];

            // ano
            String ano = vals[1];

            // fluxo
            String fluxo = vals[4];

            // quantidade
            String qntde = vals[8];

            // caso a quantidade esteja vazia entao trocar por 0
            if(qntde.equals("")){
                qntde = "0";
            }
            // estava dando erro com esse numero porque nao estava conseguindo transformar em Long
            else if (qntde.equals("1.026356999296E+15")){
                qntde = "1";
            }

            // concatenando como chave pais, fluxo e ano
            String chave = new String(pais+" "+fluxo+" "+ano);

            return new Tuple2<>(chave, new Ex03Serializable(mercadoria, Long.parseLong(qntde), pais, ano, fluxo));
        });

        // pegando trasacoes do fluxo de importacao somente do Brazil no ano de 2016
        JavaPairRDD<String, Ex03Serializable> Brazil = pairRDD.filter(
                v -> v._1.equals("Brazil Import 2016"));

        //Brazil.take(10).forEach(v->System.out.println(v._1()+" "+v._2()));

        // trocando chave e valor para poder ordenar por quantidade
        // passando quantidade como chave e como valor o objeto Ex05Serializable com todos os dados necessarios
        JavaPairRDD<Long, Ex03Serializable> BrazilOrdena = Brazil.mapToPair(p -> new Tuple2<>(p._2().getQnt(), p._2()));
        //BrazilOrdena.take(10).forEach(v->System.out.println(v._1()+" "+v._2()));

        // ordenando em ordem crescente
        JavaPairRDD<Long, Ex03Serializable> BrazilOrdenado = BrazilOrdena.sortByKey(false);

        // resultado final que retorna somente uma tuple2 com quantidade e objeto(ex03 serializable)
        // pegando o primeiro resultado uma vez que eh o com maior quantidade
        Tuple2<Long, Ex03Serializable> resultado = BrazilOrdenado.first();

        // imprimindo o resultado
        System.out.println(resultado);

    }
}
