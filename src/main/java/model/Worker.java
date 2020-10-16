package model;

import java.time.ZonedDateTime;
import java.util.Date;

public class Worker {

    private long id; //Значение поля должно быть больше 0, Значение этого поля должно быть уникальным,
    // Значение этого поля должно генерироваться автоматически
    private String name; //Поле не может быть null, Строка не может быть пустой
    private Coordinates coordinates; //Поле не может быть null
    private ZonedDateTime creationDate; //Поле не может быть null, Значение этого поля должно генерироваться автоматически
    private Double salary; //Поле может быть null, Значение поля должно быть больше 0
    private Date endDate; //Поле может быть null
    private Position position; //Поле не может быть null
    private Status status; //Поле может быть null
    private Organization organization; //Поле не может быть null

    public Worker(String name, Double x, ZonedDateTime creationDate, Double salary, Date endDate, String position, String status,
                  Integer annualTurnover, int employeesCount, String organizationType) {
        this(name, new Coordinates(x), creationDate, salary, endDate, position, status, annualTurnover, employeesCount, organizationType);
    }

    public Worker(String name, Double x, double y, ZonedDateTime creationDate, Double salary, Date endDate, String position, String status,
                  Integer annualTurnover, int employeesCount, String organizationType) {
        this(name, new Coordinates(x, y), creationDate, salary, endDate, position, status, annualTurnover, employeesCount, organizationType);
    }

    public Worker(String name, Coordinates coordinates, ZonedDateTime creationDate, Double salary, Date endDate, String position, String status,
                  Integer annualTurnover, int employeesCount, String organizationType) {
        this.name = name;
        this.coordinates = coordinates;
        this.creationDate = creationDate;
        this.salary = salary;
        this.endDate = endDate;
        this.position = Position.getByTitle(position);
        this.status = Status.getByTitle(status);
        this.organization = new Organization(annualTurnover, employeesCount, organizationType);
        validate();
    }

    private void validate() {
        if (name.isEmpty()) throw new IllegalArgumentException("Parameter name must not be empty");
        if (salary != null && salary <= 0) throw new IllegalArgumentException("Parameter salary must be more 0 or null");
        if (this.position == null) throw new IllegalArgumentException("Parameter position must be in: "
                + String.join(", ", Position.getAll()) +" or empty string");
    }

    public String convertToXML() {
        return "<worker>" +
                "<id>" + this.id + "</id>" +
                "<name>" + this.name + "</name>" +
                "<coordinateX>" + this.coordinates.getX() + "</coordinateX>" +
                "<coordinateY>" + this.coordinates.getY() + "</coordinateY>" +
                "<creationDate>" + this.creationDate + "</creationDate>" +
                "<salary>" + this.salary + "</salary>" +
                "<endDate>" + this.endDate + "</endDate>" +
                "<position>" + this.position.getTitle() + "</position>" +
                "<status>" + this.status.getTitle() + "</status>" +
                "<annualTurnover>" + this.organization.getAnnualTurnover() + "</annualTurnover>" +
                "<employeesCount>" + this.organization.getEmployeesCount() + "</employeesCount>" +
                "<organizationType>" + this.organization.getType().getTitle() + "</organizationType>" +
                "</worker>";
    }

    public Coordinates getCoordinates() {
        return coordinates;
    }

    public Date getEndDate() {
        return endDate;
    }

    public Double getSalary() {
        return salary;
    }

    public long getId() {
        return id;
    }

    public Organization getOrganization() {
        return organization;
    }

    public Position getPosition() {
        return position;
    }

    public Status getStatus() {
        return status;
    }

    public String getName() {
        return name;
    }

    public ZonedDateTime getCreationDate() {
        return creationDate;
    }

    public Worker setCoordinates(Coordinates coordinates) {
        this.coordinates = coordinates;
        return this;
    }

    public Worker setCreationDate(ZonedDateTime creationDate) {
        this.creationDate = creationDate;
        return this;
    }

    public Worker setEndDate(Date endDate) {
        this.endDate = endDate;
        return this;
    }

    public Worker setId(long id) {
        this.id = id;
        return this;
    }

    public Worker setName(String name) {
        this.name = name;
        return this;
    }

    public Worker setOrganization(Organization organization) {
        this.organization = organization;
        return this;
    }

    public Worker setPosition(Position position) {
        this.position = position;
        return this;
    }

    public Worker setSalary(Double salary) {
        this.salary = salary;
        return this;
    }

    public Worker setStatus(Status status) {
        this.status = status;
        return this;
    }
}
