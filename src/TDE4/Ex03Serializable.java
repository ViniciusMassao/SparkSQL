package TDE4;

import java.io.Serializable;

public class Ex03Serializable implements Serializable {
    private int hs_cpf;
    private int estimativa_renda;

    public Ex03Serializable() {}

    public Ex03Serializable(int hs_cpf, int estimativa_renda) {
        this.hs_cpf = hs_cpf;
        this.estimativa_renda = estimativa_renda;
    }

    public int getHs_cpf() {
        return hs_cpf;
    }

    public void setHs_cpf(int hs_cpf) {
        this.hs_cpf = hs_cpf;
    }

    public int getEstimativa_renda() {
        return estimativa_renda;
    }

    public void setEstimativa_renda(int estimativa_renda) {
        this.estimativa_renda = estimativa_renda;
    }

    @Override
    public String toString() {
        return "Ex03Serializable{" +
                "hs_cpf=" + hs_cpf +
                ", estimativa_renda=" + estimativa_renda +
                '}';
    }
}
