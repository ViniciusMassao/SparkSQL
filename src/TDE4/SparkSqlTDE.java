package sql;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.*;

import java.util.ArrayList;

import static org.apache.spark.sql.functions.*;

public class SparkSqlTDE {

    public static void main(String args[]){
        // Sets ERROR-only logging
        Logger.getLogger("org").setLevel(Level.ERROR);

        // inicializando sessao com duas threads
        SparkSession session = SparkSession.builder().
                appName("stackoverflow").
                master("local[2]").getOrCreate();

        // carregando dados em data frame
        DataFrameReader dfr = session.read();

        Dataset<Row> treino = dfr.option("header","true").
                option("inferSchema","true").
                csv("in/ommlbd_basico.csv");

        Dataset<Row> teste = dfr.option("header","true").
                option("inferSchema","true").
                csv("in/ommlbd_basico.csv");

        Dataset<Row> empresarial = dfr.option("header","true").
                option("inferSchema","true").
                csv("in/ommlbd_empresarial.csv");
        Dataset<Row> familiar = dfr.option("header","true").
                option("inferSchema","true").
                csv("in/ommlbd_familiar.csv");
        Dataset<Row> regional = dfr.option("header","true").
                option("inferSchema","true").
                csv("in/ommlbd_regional.csv");
        Dataset<Row> renda = dfr.option("header","true").
                option("inferSchema","true").
                csv("in/ommlbd_renda.csv");



        System.out.println("================= TREINO ==================");
        //Utilizando a parte de treino
        //filtrando a base
        ArrayList<String> a = new ArrayList<String>();
        a.add("DISTCENTROCIDADE");
        a.add("DISTZONARISCO");
        a.add("QTDENDERECO");
        a.add("QTDEMAIL");
        a.add("QTDCELULAR");
        a.add("CELULARPROCON");
        a.add("QTDFONEFIXO");
        a.add("TELFIXOPROCON");
        a.add("TARGET");
        a.add("ESTIMATIVARENDA");
        a.add("QTDDECLARACAOISENTA");
        a.add("QTDDECLARACAO10");
        a.add("QTDDECLARACAOREST10");
        a.add("QTDDECLARACAOPAGAR10");
        a.add("RESTITUICAOAGENCIAALTARENDA");
        a.add("BOLSAFAMILIA");
        a.add("ANOSULTIMARESTITUICAO");
        a.add("ANOSULTIMADECLARACAO");
        a.add("ANOSULTIMADECLARACAOPAGAR");
        a.add("INDICEEMPREGO");
        a.add("PORTEEMPREGADOR");
        a.add("SOCIOEMPRESA");
        a.add("FUNCIONARIOPUBLICO");
        a.add("SEGMENTACAO");
        a.add("SEGMENTACAOCOBRANCA");
        a.add("SEGMENTACAOECOM");
        a.add("SEGMENTACAOFIN");
        a.add("SEGMENTACAOTELECOM");
        a.add("QTDPESSOASCASA");
        a.add("MENORRENDACASA");
        a.add("MAIORRENDACASA");
        a.add("SOMARENDACASA");
        a.add("MEDIARENDACASA");
        a.add("MAIORIDADECASA");
        a.add("MENORIDADECASA");
        a.add("MEDIAIDADECASA");
        a.add("INDICMENORDEIDADE");
        a.add("COBRANCABAIXOCASA");
        a.add("COBRANCAMEDIOCASA");
        a.add("COBRANCAALTACASA");
        a.add("SEGMENTACAOFINBAIXACASA");
        a.add("SEGMENTACAOFINMEDIACASA");
        a.add("SEGMENTACAOALTACASA");
        a.add("BOLSAFAMILIACASA");
        a.add("FUNCIONARIOPUBLICOCASA");
        a.add("IDADEMEDIACEP");
        a.add("PERCENTMASCCEP");
        a.add("PERCENTFEMCEP");
        a.add("PERCENTANALFABETOCEP");
        a.add("PERCENTPRIMARIOCEP");
        a.add("PERCENTFUNDAMENTALCEP");
        a.add("PERCENTMEDIOCEP");
        a.add("PERCENTSUPERIORCEP");
        a.add("PERCENTMESTRADOCEP");
        a.add("PERCENTDOUTORADOCEP");
        a.add("PERCENTBOLSAFAMILIACEP");
        a.add("PERCENTFUNCIONARIOPUBLICOCEP");
        a.add("MEDIARENDACEP");
        a.add("PIBMUNICIPIO");
        a.add("QTDUTILITARIOMUNICIPIO");
        a.add("QTDAUTOMOVELMUNICIPIO");
        a.add("QTDCAMINHAOMUNICIPIO");
        a.add("QTDCAMINHONETEMUNICIPIO");
        a.add("QTDMOTOMUNICIPIO");
        a.add("PERCENTPOPZONAURBANA");
        a.add("IDHMUNICIPIO");


        treino = treino.filter(col("SAFRA").equalTo("TREINO"));
        treino = treino.filter(col("TEMPOCPF").$greater$eq(0));

        //inner na empresarial com filtro
        treino = treino.join(empresarial,"HS_CPF");

        //inner na familiar
        treino = treino.join(familiar,"HS_CPF");

        //inner na regional
        treino = treino.join(regional,"HS_CPF");

        //inner na renda
        treino = treino.join(renda,"HS_CPF");

        for(int x = 0;x<a.size();x++){
            treino = treino.filter(col(a.get(x)).$greater$eq(0));
        }
        treino.show(5);

        //numero de clientes por orientacao sexual
        System.out.println("1. O número de clientes por orientação sexual:");
        treino.groupBy("ORIENTACAO_SEXUAL").agg(count("*")).show();

        //numero maximo e minimo de emails
        System.out.println("2. O número máximo e mínimo de e-mails cadastrados em toda a base de dados:");
        System.out.println("Minimo:");
        treino.select(col("QTDEMAIL")).orderBy(col("QTDEMAIL").asc()).show(1);
        System.out.println("Maximo:");
        treino.select(col("QTDEMAIL")).orderBy(col("QTDEMAIL").desc()).show(1);

        //propostas de clientes com renda maior que 10.000
        System.out.println("3. O número de propostas cujo cliente possui estimativa de renda seja superior a R$\n" +
                "10.000,00 (dez mil reais);");
        treino.filter(col("ESTIMATIVARENDA").$greater(10000)).agg(count("*")).show();

        //proposta de clientes que fazem parte da bolsa familia
        System.out.println("4. O número de propostas de crédito cujo cliente é beneficiário do bolsa família\n" +
                "(BOLSAFAMILIA);");
        treino.filter(col("BOLSAFAMILIA").equalTo(1)).agg(count("*")).show();


        //percentual de propostas cujo cliente possui funcionario publico em casa
        System.out.println("5. O percentual de propostas de crédito cujo cliente possui um funcionário público em casa\n" +
                "(use a coluna FUNCIONARIOPUBLICOCASA);");


        treino.agg(sum("FUNCIONARIOPUBLICOCASA").$div(count("FUNCIONARIOPUBLICOCASA")).multiply(100)).show();


        //nao ta funcionando ------------------------
        System.out.println("6. O percentual de propostas de crédito cujo cliente vive em uma cidade com IDH em cada\n" +
                "uma das faixas: 0 a 10, 10 a 20, 20 a 30, 30 a 40, 40 a 50, 50 a 60, 60 a 70, 70 a 80, 80 a\n" +
                "90 e 90 a 100;");
        long max = treino.select(col("IDHMUNICIPIO")).count();
        int j =10;
        for(int i = 0; i<= 90; i=i+10){
            System.out.println(i +" ate " + j);
            treino.filter(col("IDHMUNICIPIO").between(i, j)).agg(count("*").divide(max).multiply(100)).show();
            j = j+10;
        }

        //nao ta funcionando ------------------------

        //questao 7
        System.out.println("7. O número de propostas de clientes que vivem próximos de uma região de risco (isto é,\n" +
                "cuja distância para a zona de risco mais próxima é menor que 5km) e que possuam renda\n" +
                "maior que R$ 7.000,00 (Sete mil reais);");

        treino.filter(col("DISTZONARISCO").$less(5000).and(col("ESTIMATIVARENDA").$greater(7000))).agg(count("*")).show();

        System.out.println("8. O número de propostas de clientes adimplentes e inadimplentes (TARGET) que possuem\n" +
                "renda maior que R$ 5.000,00 (cinco mil reais) e também se eles são sócios de empresas\n" +
                "ou não (SOCIOEMPRESA);");

        treino.filter(col("ESTIMATIVARENDA").$greater(5000)).groupBy("TARGET","SOCIOEMPRESA").agg(count("*")).show();



        System.out.println("================= TESTE ==================");


        //Utilizando a parte de teste
        //filtrando a base

        teste = teste.filter(col("SAFRA").equalTo("TESTE"));
        teste = teste.filter(col("TEMPOCPF").$greater$eq(0));

        //inner na empresarial
        teste = teste.join(empresarial,"HS_CPF");

        //inner na familiar
        teste = teste.join(familiar,"HS_CPF");

        //inner na regional
        teste = teste.join(regional,"HS_CPF");

        //inner na renda
        teste = teste.join(renda,"HS_CPF");

        for(int x = 0;x<a.size();x++){
            teste = teste.filter(col(a.get(x)).$greater$eq(0));
        }

        teste.show(5);

        //numero de clientes por orientacao sexual
        System.out.println("1. O número de clientes por orientação sexual:");
        teste.groupBy("ORIENTACAO_SEXUAL").agg(count("*")).show();

        //numero maximo e minimo de emails
        System.out.println("2. O número máximo e mínimo de e-mails cadastrados em toda a base de dados:");
        System.out.println("Minimo:");
        teste.select(col("QTDEMAIL")).orderBy(col("QTDEMAIL").asc()).show(1);
        System.out.println("Maximo:");
        teste.select(col("QTDEMAIL")).orderBy(col("QTDEMAIL").desc()).show(1);

        //propostas de clientes com renda maior que 10.000
        System.out.println("3. O número de propostas cujo cliente possui estimativa de renda seja superior a R$\n" +
                "10.000,00 (dez mil reais);");
        teste.filter(col("ESTIMATIVARENDA").$greater(10000)).agg(count("*")).show();

        //proposta de clientes que fazem parte da bolsa familia
        System.out.println("4. O número de propostas de crédito cujo cliente é beneficiário do bolsa família\n" +
                "(BOLSAFAMILIA);");
        teste.filter(col("BOLSAFAMILIA").equalTo(1)).agg(count("*")).show();


        //percentual de propostas cujo cliente possui funcionario publico em casa
        System.out.println("5. O percentual de propostas de crédito cujo cliente possui um funcionário público em casa\n" +
                "(use a coluna FUNCIONARIOPUBLICOCASA);");


        teste.agg(sum("FUNCIONARIOPUBLICOCASA").$div(count("FUNCIONARIOPUBLICOCASA")).multiply(100)).show();



        System.out.println("6. O percentual de propostas de crédito cujo cliente vive em uma cidade com IDH em cada\n" +
                "uma das faixas: 0 a 10, 10 a 20, 20 a 30, 30 a 40, 40 a 50, 50 a 60, 60 a 70, 70 a 80, 80 a\n" +
                "90 e 90 a 100;");

        long max2 = teste.select(col("IDHMUNICIPIO")).count();
        j =10;
        for(int i = 0; i<= 90; i=i+10){
            System.out.println(i +" ate " + j);
            teste.filter(col("IDHMUNICIPIO").between(i, j)).agg(count("*").divide(max2).multiply(100)).show();
            j = j+10;
        }


        //questao 7
        System.out.println("7. O número de propostas de clientes que vivem próximos de uma região de risco (isto é,\n" +
                "cuja distância para a zona de risco mais próxima é menor que 5km) e que possuam renda\n" +
                "maior que R$ 7.000,00 (Sete mil reais);");

        teste.filter(col("DISTZONARISCO").$less(5000).and(col("ESTIMATIVARENDA").$greater(7000))).agg(count("*")).show();

        //questao 8
        System.out.println("8. O número de propostas de clientes adimplentes e inadimplentes (TARGET) que possuem\n" +
                "renda maior que R$ 5.000,00 (cinco mil reais) e também se eles são sócios de empresas\n" +
                "ou não (SOCIOEMPRESA);");

        teste.filter(col("ESTIMATIVARENDA").$greater(5000)).groupBy("TARGET","SOCIOEMPRESA").agg(count("*")).show();

        // parando a sessão
        session.stop();

    }

}
