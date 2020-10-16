package model;

import java.util.ArrayList;
import java.util.Arrays;

public enum Status {
    HIRED("hired"),
    RECOMMENDED_FOR_PROMOTION("recommended_for_promotion"),
    REGULAR("regular"),
    PROBATION("probation");

    private final String title;

    Status(String title) {
        this.title = title;
    }

    public String getTitle() {
        return title;
    }

    public static ArrayList<String> getAll() {
        return new ArrayList<>(Arrays.asList("hired", "recommended_for_promotion", "regular", "probation", "null"));
    }

    public static Status getByTitle(String title) {
        switch (title) {
            case "hired": return HIRED;
            case "recommended_for_promotion": return RECOMMENDED_FOR_PROMOTION;
            case "regular": return REGULAR;
            case "probation": return PROBATION;
            case "":
            case "null": return null;
            default: throw new IllegalArgumentException("Parameter status must be in: "
                    + String.join(", ", Status.getAll()) +"!");
        }
    }
}

