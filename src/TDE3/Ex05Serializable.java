package TDE3;

import java.io.Serializable;

public class Ex05Serializable implements Serializable {
    //variavel de peso
    private float peso;
    //variavel de quantidade de ocorrencias
    private float qnt;

    //construtor vazio
    public Ex05Serializable(){}

    //construtor
    public Ex05Serializable(float peso, float qnt) {
        this.peso = peso;
        this.qnt = qnt;
    }

    //getter de peso
    public float getPeso() {
        return peso;
    }

    //getter de quantidade
    public float getQnt() {
        return qnt;
    }

    //setter de peso
    public void setPeso(float peso) {
        this.peso = peso;
    }

    //setter de quantidade
    public void setQnt(float qnt) {
        this.qnt = qnt;
    }

    @Override
    public String toString() {
        return "Ex04Writable{" +
                "peso=" + peso +
                ", qnt=" + qnt +
                '}';
    }
}

