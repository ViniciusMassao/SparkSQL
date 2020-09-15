package TDE3;

import java.io.Serializable;

public class Ex07Serializable implements Serializable {
    private String fluxo;
    private float qnt;

    public Ex07Serializable() {}

    public Ex07Serializable(String fluxo, float qnt) {
        this.fluxo = fluxo;
        this.qnt = qnt;
    }

    public String getFluxo() {
        return fluxo;
    }

    public void setFluxo(String fluxo) {
        this.fluxo = fluxo;
    }

    public float getQnt() {
        return qnt;
    }

    public void setQnt(float qnt) {
        this.qnt = qnt;
    }

    @Override
    public String toString() {
        return "Ex07Serializable{" +
                "fluxo='" + fluxo + '\'' +
                ", qnt=" + qnt +
                '}';
    }
}
