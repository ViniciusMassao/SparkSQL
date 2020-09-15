package sql;

import java.io.Serializable;

public class Response implements Serializable {
    private String country;
    private Integer ageMidPoint; // camel case
    private String occupation;
    private Double salaryMidPoint;

    // nao esquecam destes metodos
    // construtor vazio
    // gets e sets

    public Response(){}

    public Response(String country, Integer ageMidPoint, String occupation, Double salaryMidPoint) {
        this.country = country;
        this.ageMidPoint = ageMidPoint;
        this.occupation = occupation;
        this.salaryMidPoint = salaryMidPoint;
    }

    public String getCountry() {
        return country;
    }

    public Integer getAgeMidPoint() {
        return ageMidPoint;
    }

    public String getOccupation() {
        return occupation;
    }

    public Double getSalaryMidPoint() {
        return salaryMidPoint;
    }

    public void setCountry(String country) {
        this.country = country;
    }

    public void setAgeMidPoint(Integer ageMidPoint) {
        this.ageMidPoint = ageMidPoint;
    }

    public void setOccupation(String occupation) {
        this.occupation = occupation;
    }

    public void setSalaryMidPoint(Double salaryMidPoint) {
        this.salaryMidPoint = salaryMidPoint;
    }

    @Override
    public String toString() {
        return "Response{" +
                "country='" + country + '\'' +
                ", ageMidPoint=" + ageMidPoint +
                ", occupation='" + occupation + '\'' +
                ", salaryMidPoint=" + salaryMidPoint +
                '}';
    }
}
