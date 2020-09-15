package TDE4;

public class Ex05Serializable {
    private int hs_cpf;
    private int funcionarioPublico;

    public Ex05Serializable() {
    }

    public Ex05Serializable(int hs_cpf, int funcionarioPublico) {
        this.hs_cpf = hs_cpf;
        this.funcionarioPublico = funcionarioPublico;
    }

    public int getHs_cpf() {
        return hs_cpf;
    }

    public void setHs_cpf(int hs_cpf) {
        this.hs_cpf = hs_cpf;
    }

    public int getFuncionarioPublico() {
        return funcionarioPublico;
    }

    public void setFuncionarioPublico(int funcionarioPublico) {
        this.funcionarioPublico = funcionarioPublico;
    }
}
