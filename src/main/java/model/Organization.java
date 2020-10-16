package model;

public class Organization {
    private Integer annualTurnover; //Поле не может быть null, Значение поля должно быть больше 0
    private int employeesCount; //Значение поля должно быть больше 0
    private OrganizationType type; //Поле не может быть null

    public Organization(Integer annualTurnover, String type) {
        this(annualTurnover, 1, type);
    }

    public Organization(Integer annualTurnover, int employeesCount, String type) {
        this.annualTurnover = annualTurnover;
        this.employeesCount = employeesCount;
        this.type = OrganizationType.getByTitle(type);
        if (employeesCount <= 0) throw new IllegalArgumentException("Parameter employeesCount must be more 0");
        if (annualTurnover == null) throw new IllegalArgumentException("Parameter annualTurnover must not be null");
        if (this.type == null) throw new IllegalArgumentException("Parameter organizationType must be in: "
                + String.join(", ", OrganizationType.getAll()));
    }

    public int getEmployeesCount() {
        return employeesCount;
    }

    public Integer getAnnualTurnover() {
        return annualTurnover;
    }

    public OrganizationType getType() {
        return type;
    }

    public Organization setAnnualTurnover(Integer annualTurnover) {
        this.annualTurnover = annualTurnover;
        return this;
    }

    public Organization setEmployeesCount(int employeesCount) {
        if (employeesCount <= 0) throw new IllegalArgumentException("Parameter employeesCount must be more 0");
        this.employeesCount = employeesCount;
        return this;
    }

    public Organization setType(String type) {
        return setType(OrganizationType.getByTitle(type));
    }

    public Organization setType(OrganizationType type) {
        if (type == null) throw new IllegalArgumentException("Parameter organizationType must be in: "
                + String.join(", ", OrganizationType.getAll()));
        this.type = type;
        return this;
    }
}
