package TDE3;

import java.io.Serializable;

public class Ex06Serializable implements Serializable {

    //mercadoria
    private String mercadoria;
    //valor do preco
    private float preco;
    //unidade de medida
    private String unidade;

    //construtor vazio
    public Ex06Serializable() {}

    public Ex06Serializable(String mercadoria, float preco, String unidade) {
        this.mercadoria = mercadoria;
        this.preco = preco;
        this.unidade = unidade;
    }

    public String getMercadoria() {
        return mercadoria;
    }

    public void setMercadoria(String mercadoria) {
        this.mercadoria = mercadoria;
    }

    public float getPreco() {
        return preco;
    }

    public void setPreco(float preco) {
        this.preco = preco;
    }

    public String getUnidade() {
        return unidade;
    }

    @Override
    public String toString() {
        return "Ex06Serializable{" +
                "mercadoria='" + mercadoria + '\'' +
                ", preco=" + preco +
                ", unidade='" + unidade + '\'' +
                '}';
    }

    public void setUnidade(String unidade) {
        this.unidade = unidade;
    }


}
