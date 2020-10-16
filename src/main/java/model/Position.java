package model;

import java.util.ArrayList;
import java.util.Arrays;

public enum Position {
    LABORER("laborer"),
    HUMAN_RESOURCES("human_resources"),
    CLEANER("cleaner"),
    MANAGER_OF_CLEANING("manager_of_cleaning");

    private final String title;

    Position(String title) {
        this.title = title;
    }

    public String getTitle() {
        return title;
    }

    public static ArrayList<String> getAll() {
        return new ArrayList<>(Arrays.asList("laborer", "human_resources", "cleaner", "manager_of_cleaning"));
    }

    public static Position getByTitle(String title) {
        switch (title) {
            case "laborer": return LABORER;
            case "human_resources": return HUMAN_RESOURCES;
            case "cleaner": return CLEANER;
            case "manager_of_cleaning": return MANAGER_OF_CLEANING;
            default: return null;
        }
    }
}

