import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.List;

public class MainPerson {
    public static void main(String []args) throws IOException {
        List<Person> list = Arrays.asList(
                new Person("Joao", 20),
                new Person("Maria", 30),
                new Person("Fulano", 55)
        );

        //printando todo mundo
        list.forEach(pessoa ->System.out.println("Nome: "+pessoa.getName()+" Idade: "+pessoa.getAge()));
        System.out.println("");
        //printando aqueles que tem idade maior que 20 anos
        list.stream()
                .filter(i -> i.getAge() > 20)
                .forEach(pessoa ->System.out.println("Nome: "+pessoa.getName()+" Idade: "+pessoa.getAge()));

        System.out.println("Deseja procurar quem?");
        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
        String nome = reader.readLine();
        //printando caso a pessoa exista na lista
        list.stream()
                .filter(i -> i.getName().equals(nome))
                .forEach(pessoa ->System.out.println(pessoa.getName()+" existe na lista!"));
    }
}
