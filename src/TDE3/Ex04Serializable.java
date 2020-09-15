package TDE3;

import java.io.Serializable;

public class Ex04Serializable implements Serializable {
    private float qnt;
    private float peso;

    public Ex04Serializable() {}

    public Ex04Serializable(float qnt, float peso) {
        this.qnt = qnt;
        this.peso = peso;
    }

    public float getQnt() {
        return qnt;
    }

    public void setQnt(float qnt) {
        this.qnt = qnt;
    }

    public float getPeso() {
        return peso;
    }

    public void setPeso(float peso) {
        this.peso = peso;
    }

    @Override
    public String toString() {
        return "Ex04Serializable{" +
                "qnt=" + qnt +
                ", peso=" + peso +
                '}';
    }
}
