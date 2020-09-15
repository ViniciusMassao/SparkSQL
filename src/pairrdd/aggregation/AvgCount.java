package pairrdd.aggregation;

import java.io.Serializable;

public class AvgCount implements Serializable {
    private float qtd;
    private double preco;

    public AvgCount() {}

    public AvgCount(float qtd, double preco) {
        this.qtd = qtd;
        this.preco = preco;
    }

    public void setQtd(float qtd) {
        this.qtd = qtd;
    }

    public void setPreco(double preco) {
        this.preco = preco;
    }

    public float getQtd() {
        return qtd;
    }

    public double getPreco() {
        return preco;
    }

    @Override
    public String toString() {
        return "AvgCount{" +
                "qtd=" + qtd +
                ", preco=" + preco +
                '}';
    }
}
