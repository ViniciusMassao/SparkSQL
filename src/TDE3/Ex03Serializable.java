package TDE3;

import java.io.Serializable;

public class Ex03Serializable implements Serializable {
    private String mercadoria;
    private long qnt;
    private String pais;
    private String ano;
    private String fluxo;

    public Ex03Serializable(){}

    public Ex03Serializable(String mercadoria, long qnt, String pais, String ano, String fluxo) {
        this.mercadoria = mercadoria;
        this.qnt = qnt;
    }

    public String getPais() {
        return pais;
    }

    public String getAno() {
        return ano;
    }

    public String getFluxo() {
        return fluxo;
    }

    public String getMercadoria() {
        return mercadoria;
    }

    public long getQnt() {
        return qnt;
    }

    public void setMercadoria(String mercadoria) {
        this.mercadoria = mercadoria;
    }

    public void setQnt(long qnt) {
        this.qnt = qnt;
    }

    public void setPais(String pais) {
        this.pais = pais;
    }

    public void setAno(String ano) {
        this.ano = ano;
    }

    public void setFluxo(String fluxo) {
        this.fluxo = fluxo;
    }

    @Override
    public String toString() {
        return "Ex03Serializable{" +
                "mercadoria='" + mercadoria + '\'' +
                ", qnt=" + qnt +
                '}';
    }
}
