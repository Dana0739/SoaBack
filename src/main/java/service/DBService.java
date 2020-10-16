package service;

import model.Worker;

import java.sql.*;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Date;
import java.sql.Timestamp;

public class DBService {

    static final String DB_URL = "jdbc:postgresql://127.0.0.1:5432/postgres";
    static final String USER = "postgres";
    static final String PASS = "123";

    public static Connection getConnection() throws SQLException {
        try {
            Class.forName("org.postgresql.Driver");
        } catch (ClassNotFoundException e) {
            System.out.println("PostgreSQL JDBC Driver is not found. Include it in your library path ");
            e.printStackTrace();
            return null;
        }

        System.out.println("PostgreSQL JDBC Driver successfully connected");
        Connection connection = null;

        try {
            connection = DriverManager.getConnection(DB_URL, USER, PASS);
        } catch (SQLException e) {
            System.out.println("Connection Failed");
            e.printStackTrace();
            return null;
        }

        if (connection != null) {
            System.out.println("You successfully connected to database now");

            DatabaseMetaData meta = connection.getMetaData();
            ResultSet res = meta.getTables(null, null, "worker", null);
            if (!res.next()) createTable(connection);

        } else {
            System.out.println("Failed to make connection to database");
        }

        return connection;
    }

    private static void createTable(Connection connection) throws SQLException {
        Statement statement = connection.createStatement();
        String sql = "CREATE TABLE WORKER " +
                "(ID                    SERIAL                      PRIMARY KEY," +
                " NAME                  TEXT                        NOT NULL, " +
                " COORDINATE_X          REAL                        NOT NULL, " +
                " COORDINATE_Y          REAL, " +
                " CREATION_DATE         TIMESTAMP WITH TIME ZONE    NOT NULL, " +
                " SALARY                REAL, " +
                " END_DATE              DATE, " +
                " POSITION              CHAR(30)                    NOT NULL, " +
                " STATUS                CHAR(30), " +
                " O_ANNUAL_TURNOVER     INT                         NOT NULL," +
                " O_EMPLOYEES_COUNT     INT                         NOT NULL," +
                " O_ORGANIZATION_TYPE   CHAR(30)                    NOT NULL)";
        statement.executeUpdate(sql);
        statement.close();
    }

    public static Worker getWorkerById(Connection connection, long id) throws SQLException {
        PreparedStatement select = connection.prepareStatement("SELECT * FROM WORKER WHERE ID = ?;");
        select.setLong(1, id);
        ResultSet rs = select.executeQuery();
        Worker worker = null;
        if(rs.next()) {
            String name = rs.getString("NAME");
            Double x = rs.getDouble("COORDINATE_X");
            double y = rs.getDouble("COORDINATE_Y");
            ZonedDateTime creationDate = ZonedDateTime.ofInstant(rs.getTimestamp("CREATION_DATE").toInstant(),
                    ZoneId.systemDefault());
            Double salary = rs.getDouble("SALARY");
            Date endDate = rs.getTimestamp("END_DATE");
            String position = rs.getString("POSITION").trim();
            String status = rs.getString("STATUS").trim();
            Integer annualTurnover = rs.getInt("O_ANNUAL_TURNOVER");
            int employeesCount = rs.getInt("O_EMPLOYEES_COUNT");
            String organizationType = rs.getString("O_ORGANIZATION_TYPE").trim();

            worker = new Worker(name, x, y, creationDate, salary, endDate,
                    position, status, annualTurnover, employeesCount, organizationType).setId(id);
        }
        rs.close();
        connection.close();
        return worker;
    }

    public static ArrayList<Worker> getAllWorkers(Connection connection) throws SQLException {
        Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery( "SELECT * FROM WORKER;");
        ArrayList<Worker> workers = new ArrayList<>();

        while (rs.next()) {
            long id = rs.getLong("ID");
            String name = rs.getString("NAME");
            Double x = rs.getDouble("COORDINATE_X");
            double y = rs.getDouble("COORDINATE_Y");
            ZonedDateTime creationDate = ZonedDateTime.ofInstant(rs.getTimestamp("CREATION_DATE").toInstant(),
                    ZoneId.systemDefault());
            Double salary = rs.getDouble("SALARY");
            Date endDate = rs.getTimestamp("END_DATE");
            String position = rs.getString("POSITION").trim();
            String status = rs.getString("STATUS").trim();
            Integer annualTurnover = rs.getInt("O_ANNUAL_TURNOVER");
            int employeesCount = rs.getInt("O_EMPLOYEES_COUNT");
            String organizationType = rs.getString("O_ORGANIZATION_TYPE").trim();

            workers.add(new Worker(name, x, y, creationDate, salary, endDate,
                    position, status, annualTurnover, employeesCount, organizationType).setId(id));
        }
        rs.close();
        connection.close();
        return workers;
    }

    public static Worker insertWorker(Connection connection, Worker worker) throws SQLException {
        PreparedStatement insert = connection.prepareStatement
                ("INSERT INTO WORKER (NAME, COORDINATE_X, COORDINATE_Y, CREATION_DATE, SALARY, END_DATE, " +
                        "POSITION, STATUS, O_ANNUAL_TURNOVER, O_EMPLOYEES_COUNT, O_ORGANIZATION_TYPE) " +
                        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);");

        insert.setString(1, worker.getName());
        insert.setDouble(2, worker.getCoordinates().getX());
        insert.setDouble(3, worker.getCoordinates().getY());
        insert.setTimestamp(4, java.sql.Timestamp.from(worker.getCreationDate().toInstant()));
        insert.setDouble(5, worker.getSalary());
        insert.setTimestamp(6, new Timestamp(worker.getEndDate().getTime()));
        insert.setString(7, worker.getPosition().getTitle());
        insert.setString(8, worker.getStatus().getTitle());
        insert.setInt(9, worker.getOrganization().getAnnualTurnover());
        insert.setInt(10, worker.getOrganization().getEmployeesCount());
        insert.setString(11, worker.getOrganization().getType().getTitle());
        insert.executeUpdate();

        PreparedStatement select = connection.prepareStatement("SELECT ID FROM WORKER WHERE CREATION_DATE = ?;");
        select.setTimestamp(1, java.sql.Timestamp.from(worker.getCreationDate().toInstant()));
        ResultSet rs = select.executeQuery();
        rs.next();
        worker.setId(rs.getLong("ID"));

        insert.close();
        select.close();
        connection.close();
        return worker;
    }

    public static Worker updateWorker(Connection connection, Worker worker) throws SQLException {
        PreparedStatement update = connection.prepareStatement
                ("UPDATE WORKER SET NAME = ?, COORDINATE_X = ?, COORDINATE_Y = ?, CREATION_DATE = ?, " +
                        "SALARY = ?, END_DATE = ?, POSITION = ?, STATUS = ?, O_ANNUAL_TURNOVER = ?, " +
                        "O_EMPLOYEES_COUNT = ?, O_ORGANIZATION_TYPE = ? WHERE id = ?;");

        update.setString(1, worker.getName());
        update.setDouble(2, worker.getCoordinates().getX());
        update.setDouble(3, worker.getCoordinates().getY());
        update.setTimestamp(4, java.sql.Timestamp.from(worker.getCreationDate().toInstant()));
        update.setDouble(5, worker.getSalary());
        update.setTimestamp(6, new Timestamp(worker.getEndDate().getTime()));
        update.setString(7, worker.getPosition().getTitle());
        update.setString(8, worker.getStatus().getTitle());
        update.setInt(9, worker.getOrganization().getAnnualTurnover());
        update.setInt(10, worker.getOrganization().getEmployeesCount());
        update.setString(11, worker.getOrganization().getType().getTitle());
        update.setLong(12, worker.getId());

        update.executeUpdate();

        update.close();
        connection.close();
        return worker;
    }

    public static Worker deleteWorker(Connection connection, long id) throws SQLException {
        PreparedStatement select = connection.prepareStatement("SELECT * FROM WORKER WHERE ID = ?;");
        select.setLong(1, id);
        ResultSet rs = select.executeQuery();
        rs.next();

        String name = rs.getString("NAME");
        Double x = rs.getDouble("COORDINATE_X");
        double y = rs.getDouble("COORDINATE_Y");
        ZonedDateTime creationDate = ZonedDateTime.ofInstant(rs.getTimestamp("CREATION_DATE").toInstant(),
                ZoneId.systemDefault());
        Double salary = rs.getDouble("SALARY");
        Date endDate = rs.getTimestamp("END_DATE");
        String position = rs.getString("POSITION").trim();
        String status = rs.getString("STATUS").trim();
        Integer annualTurnover = rs.getInt("O_ANNUAL_TURNOVER");
        int employeesCount = rs.getInt("O_EMPLOYEES_COUNT");
        String organizationType = rs.getString("O_ORGANIZATION_TYPE").trim();

        Worker worker = new Worker(name, x, y, creationDate, salary, endDate,
                position, status, annualTurnover, employeesCount, organizationType).setId(id);

        PreparedStatement delete = connection.prepareStatement("DELETE FROM WORKER WHERE ID = ?;");
        delete.setLong(1, id);
        delete.executeUpdate();

        delete.close();
        select.close();
        connection.close();
        return worker;
    }
}
