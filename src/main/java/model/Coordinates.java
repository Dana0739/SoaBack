package model;

import static java.lang.Math.min;

public class Coordinates {
    private Double x; //Поле не может быть null
    private double y; //Максимальное значение поля: 444

    public Coordinates(Double x) {
        this(x, 0);
    }

    public Coordinates(Double x, double y) {
        this.x = x;
        this.y = min(y, 444);
        if (this.x == null) throw new IllegalArgumentException("Parameter x must not be null");
    }

    public Double getX() {
        return x;
    }

    public double getY() {
        return y;
    }

    public Coordinates setX(Double x) {
        if (x == null) throw new IllegalArgumentException("Parameter x must not be null");
        this.x = x;
        return this;
    }

    public Coordinates setY(double y) {
        this.y = min(y, 444);
        return this;
    }
}
