package model;

import java.util.ArrayList;
import java.util.Arrays;

public enum OrganizationType {
    PUBLIC("public"),
    GOVERNMENT("government"),
    TRUST("trust"),
    PRIVATE_LIMITED_COMPANY("private_limited_company"),
    OPEN_JOINT_STOCK_COMPANY("open_joint_stock_company");

    private final String title;

    OrganizationType(String title) {
        this.title = title;
    }

    public String getTitle() {
        return title;
    }

    public static ArrayList<String> getAll() {
        return new ArrayList<>(Arrays.asList("public", "government", "trust", "private_limited_company", "open_joint_stock_company"));
    }

    public static OrganizationType getByTitle(String title) {
        switch (title) {
            case "public": return PUBLIC;
            case "government": return GOVERNMENT;
            case "trust": return TRUST;
            case "private_limited_company": return PRIVATE_LIMITED_COMPANY;
            case "open_joint_stock_company": return OPEN_JOINT_STOCK_COMPANY;
            default: return null;
        }
    }
}

