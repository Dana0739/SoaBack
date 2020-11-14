package service;

import model.Worker;

import java.sql.*;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.*;
import java.sql.Timestamp;
import java.util.Date;

public class WorkerStorage {

    static final String DB_URL = "jdbc:postgresql://localhost:5432/studs";
    static final String USER = "username";
    static final String PASS = "password";

//    public static void main(String[] args) throws SQLException {
//        Worker w = new Worker("test", 1.0, 2.0, ZonedDateTime.now(), 100.0,
//                Date.from(Instant.now()), Position.CLEANER.getTitle(), Status.HIRED.getTitle(), 11111, 100,
//                OrganizationType.GOVERNMENT.getTitle());
//        insertWorker(Objects.requireNonNull(getConnection()), w);
//        System.out.println(w.getId());
//        w.setName("Leha");
//        updateWorker(Objects.requireNonNull(getConnection()), w);
//        deleteWorker(getConnection(), w.getId());
//        ArrayList<Worker> workers1 = getAllWorkers(getConnection());
//        Worker worker = getWorkerById(getConnection(), 6);
//        String[] filterFields = new String[]{"name", "position", "status", "id", "creationDate", "employeesCount",
//                "salary", "organizationType", "annualTurnover", "endDate", "coordinateY", "coordinateX"};
//        String[] filterValues = new String[]{"DanaDana", "human_resources", "hired", "4",
//                "2020-10-16 19:49:51.825796", "2", "300", "government", "30", "3030-05-25 00:00:00.0", "2.7865", "1.3546"};
//        String[] sortFields = new String[]{"name", "position", "status", "id", "creationDate", "employeesCount",
//                "salary", "organizationType", "annualTurnover", "endDate", "coordinateY", "coordinateX"};
//        ArrayList<Worker> workers2 = getWorkers(getConnection(), filterFields, filterValues, sortFields);
//        workers2.size();
//    }

    public static Connection getConnection() throws SQLException {
        try {
            Class.forName("org.postgresql.Driver");
        } catch (ClassNotFoundException e) {
            System.err.println("PostgreSQL JDBC Driver is not found. Include it in your library path ");
            e.printStackTrace();
            return null;
        }

        System.out.println("PostgreSQL JDBC Driver successfully loaded");
        Connection connection;
        try {
            connection = DriverManager.getConnection(DB_URL, USER, PASS);
        } catch (SQLException e) {
            System.err.println("Connection Failed");
            e.printStackTrace();
            return null;
        }

        if (connection != null) {
            System.out.println("You successfully connected to database now");
            DatabaseMetaData meta = connection.getMetaData();
            ResultSet res = meta.getTables(null, null, "worker", null);
            if (!res.next()) createTable(connection);
        } else {
            System.err.println("Failed to make connection to database");
        }
        return connection;
    }

    private static void createTable(Connection connection) throws SQLException {
        Statement statement = connection.createStatement();
        String sql = "CREATE TABLE WORKER " +
                "(ID                    SERIAL                      PRIMARY KEY," +
                " NAME                  TEXT                        NOT NULL, " +
                " COORDINATE_X          REAL                        NOT NULL, " +
                " COORDINATE_Y          REAL                        NOT NULL, " +
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

    private static Worker getWorkerByIdSaveConnection(Connection connection, long id) throws SQLException {
        PreparedStatement select = connection.prepareStatement("SELECT * FROM WORKER WHERE ID = ?;");
        select.setLong(1, id);
        ResultSet rs = select.executeQuery();
        Worker worker = null;
        if (rs.next()) {
            worker = makeWorkerFromRs(rs).setId(id);
        }
        select.close();
        rs.close();
        return worker;
    }

    public static Worker getWorkerById(Connection connection, long id) throws SQLException {
        Worker worker = getWorkerByIdSaveConnection(connection, id);
        connection.close();
        return worker;
    }

    private static final Map<String, String> FIELDS = new HashMap<>();

    static {
        FIELDS.put("id", "ID");
        FIELDS.put("name", "NAME");
        FIELDS.put("coordinateX", "COORDINATE_X");
        FIELDS.put("coordinateY", "COORDINATE_Y");
        FIELDS.put("creationDate", "CREATION_DATE");
        FIELDS.put("salary", "SALARY");
        FIELDS.put("endDate", "END_DATE");
        FIELDS.put("position", "POSITION");
        FIELDS.put("status", "STATUS");
        FIELDS.put("annualTurnover", "O_ANNUAL_TURNOVER");
        FIELDS.put("employeesCount", "O_EMPLOYEES_COUNT");
        FIELDS.put("organizationType", "O_ORGANIZATION_TYPE");
    }

    public static ArrayList<Worker> getWorkers(Connection connection, String[] filterFields, String[] filterValues,
                                               String[] sortFields) throws SQLException {
        List<String> filterFieldsList = Arrays.asList(filterFields);
        StringBuilder selectBuilder = new StringBuilder("SELECT * FROM WORKER ");
        if (!filterFieldsList.isEmpty()) {
            selectBuilder.append(" WHERE ");
            for (String field : filterFieldsList) {
                selectBuilder.append(FIELDS.get(field));
                selectBuilder.append(" = ? and ");
            }
            selectBuilder.replace(selectBuilder.lastIndexOf("and"), selectBuilder.lastIndexOf("and") + 3, "");
        }

        if (sortFields.length != 0) {
            selectBuilder.append(" ORDER BY ");
            for (String field : sortFields) {
                selectBuilder.append(FIELDS.get(field));
                selectBuilder.append(", ");
            }
            selectBuilder.deleteCharAt(selectBuilder.lastIndexOf(","));
        }

        selectBuilder.append(";");
        PreparedStatement preparedStatement = connection.prepareStatement(selectBuilder.toString());

        if (filterFieldsList.contains("id")) {
            preparedStatement.setLong(filterFieldsList.indexOf("id") + 1,
                    Long.parseLong(filterValues[filterFieldsList.indexOf("id")]));
        }
        if (filterFieldsList.contains("name")) {
            preparedStatement.setString(filterFieldsList.indexOf("name") + 1,
                    filterValues[filterFieldsList.indexOf("name")]);
        }
        if (filterFieldsList.contains("coordinateX")) {
            preparedStatement.setDouble(filterFieldsList.indexOf("coordinateX") + 1,
                    Double.parseDouble(filterValues[filterFieldsList.indexOf("coordinateX")]));
        }
        if (filterFieldsList.contains("coordinateY")) {
            preparedStatement.setDouble(filterFieldsList.indexOf("coordinateY") + 1,
                    Double.parseDouble(filterValues[filterFieldsList.indexOf("coordinateY")]));
        }
        if (filterFieldsList.contains("creationDate")) {
            preparedStatement.setTimestamp(filterFieldsList.indexOf("creationDate") + 1,
                    java.sql.Timestamp.valueOf(ZonedDateTime.parse(filterValues[filterFieldsList.indexOf("creationDate")]
                            .replace(" ", "+")).toLocalDateTime()));
        }
        if (filterFieldsList.contains("salary")) {
            preparedStatement.setDouble(filterFieldsList.indexOf("salary") + 1,
                    Double.parseDouble(filterValues[filterFieldsList.indexOf("salary")]));
        }
        if (filterFieldsList.contains("endDate")) {
            preparedStatement.setTimestamp(filterFieldsList.indexOf("endDate") + 1,
                    java.sql.Timestamp.valueOf(filterValues[filterFieldsList.indexOf("endDate")]
                            .replace("T", " ") + ":00"));
        }
        if (filterFieldsList.contains("position")) {
            preparedStatement.setString(filterFieldsList.indexOf("position") + 1,
                    filterValues[filterFieldsList.indexOf("position")]);
        }
        if (filterFieldsList.contains("status")) {
            preparedStatement.setString(filterFieldsList.indexOf("status") + 1,
                    filterValues[filterFieldsList.indexOf("status")]);
        }
        if (filterFieldsList.contains("annualTurnover")) {
            preparedStatement.setInt(filterFieldsList.indexOf("annualTurnover") + 1,
                    Integer.parseInt(filterValues[filterFieldsList.indexOf("annualTurnover")]));
        }
        if (filterFieldsList.contains("employeesCount")) {
            preparedStatement.setInt(filterFieldsList.indexOf("employeesCount") + 1,
                    Integer.parseInt(filterValues[filterFieldsList.indexOf("employeesCount")]));
        }
        if (filterFieldsList.contains("organizationType")) {
            preparedStatement.setString(filterFieldsList.indexOf("organizationType") + 1,
                    filterValues[filterFieldsList.indexOf("organizationType")]);
        }

        ResultSet rs = preparedStatement.executeQuery();
        ArrayList<Worker> workers = collectWorkersFromRs(rs);
        rs.close();
        connection.close();
        return workers;
    }

    public static ArrayList<Worker> getAllWorkers(Connection connection) throws SQLException {
        Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery( "SELECT * FROM WORKER ORDER BY ID;");
        ArrayList<Worker> workers = collectWorkersFromRs(rs);
        rs.close();
        connection.close();
        return workers;
    }

    public static Worker insertWorker(Connection connection, Worker worker) throws SQLException {
        PreparedStatement insert = connection.prepareStatement
                ("INSERT INTO WORKER (NAME, COORDINATE_X, COORDINATE_Y, SALARY, END_DATE, " +
                        "POSITION, STATUS, O_ANNUAL_TURNOVER, O_EMPLOYEES_COUNT, O_ORGANIZATION_TYPE, CREATION_DATE) " +
                        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);");
        insert = setCommonParams(insert, worker);
        insert.setTimestamp(11, java.sql.Timestamp.from(worker.getCreationDate().toInstant()));
        insert.executeUpdate();
        insert.close();

        PreparedStatement select = connection.prepareStatement("SELECT ID FROM WORKER WHERE CREATION_DATE = ?;");
        select.setTimestamp(1, java.sql.Timestamp.from(worker.getCreationDate().toInstant()));
        ResultSet rs = select.executeQuery();
        rs.next();
        worker.setId(rs.getLong("ID"));
        select.close();
        rs.close();
        connection.close();
        return worker;
    }

    public static Worker updateWorker(Connection connection, Worker worker) throws SQLException {
        PreparedStatement update = connection.prepareStatement
                ("UPDATE WORKER SET NAME = ?, COORDINATE_X = ?, COORDINATE_Y = ?, " +
                        "SALARY = ?, END_DATE = ?, POSITION = ?, STATUS = ?, O_ANNUAL_TURNOVER = ?, " +
                        "O_EMPLOYEES_COUNT = ?, O_ORGANIZATION_TYPE = ? WHERE id = ?;");

        update = setCommonParams(update, worker);
        update.setLong(11, worker.getId());
        update.executeUpdate();
        update.close();

        PreparedStatement select = connection.prepareStatement("SELECT CREATION_DATE FROM WORKER WHERE ID = ?;");
        select.setLong(1, worker.getId());
        ResultSet rs = select.executeQuery();
        rs.next();
        ZonedDateTime creationDate = ZonedDateTime.ofInstant(rs.getTimestamp(1).toInstant(),
                ZoneId.systemDefault());
        worker.setCreationDate(creationDate);
        select.close();
        rs.close();
        connection.close();
        return worker;
    }

    public static boolean deleteWorker(Connection connection, long id) throws SQLException {
        PreparedStatement delete = connection.prepareStatement("DELETE FROM WORKER WHERE ID = ?;");
        delete.setLong(1, id);
        delete.executeUpdate();
        delete.close();
        Worker worker = getWorkerByIdSaveConnection(connection, id);
        connection.close();
        return worker == null;
    }

    private static Worker makeWorkerFromRs(ResultSet rs) throws SQLException {
        String name = rs.getString("NAME");
        Double x = rs.getDouble("COORDINATE_X");
        double y = rs.getDouble("COORDINATE_Y");
        ZonedDateTime creationDate = ZonedDateTime.ofInstant(rs.getTimestamp("CREATION_DATE").toInstant(),
                ZoneId.systemDefault());
        Double salary = rs.getDouble("SALARY");
        if (salary == 0) salary = null;
        Date endDate = rs.getTimestamp("END_DATE");
        String position = rs.getString("POSITION").trim();
        String status = (rs.getObject("STATUS") == null) ? null : rs.getString("STATUS").trim();
        Integer annualTurnover = rs.getInt("O_ANNUAL_TURNOVER");
        int employeesCount = rs.getInt("O_EMPLOYEES_COUNT");
        String organizationType = rs.getString("O_ORGANIZATION_TYPE").trim();

        return new Worker(name, x, y, creationDate, salary, endDate,
                position, status, annualTurnover, employeesCount, organizationType);
    }

    private static ArrayList<Worker> collectWorkersFromRs(ResultSet rs) throws SQLException {
        ArrayList<Worker> workers = new ArrayList<>();
        while (rs.next()) {
            long id = rs.getLong("ID");
            workers.add(makeWorkerFromRs(rs).setId(id));
        }
        return workers;
    }

    private static PreparedStatement setCommonParams(PreparedStatement statement, Worker worker) throws SQLException {
        statement.setString(1, worker.getName());
        statement.setDouble(2, worker.getCoordinates().getX());
        statement.setDouble(3, worker.getCoordinates().getY());
        statement.setObject(4, (worker.getSalary() == null) ? null : worker.getSalary());
        statement.setObject(5, (worker.getEndDate() == null) ? null : new Timestamp(worker.getEndDate().getTime()));
        statement.setString(6, worker.getPosition().getTitle());
        statement.setObject(7, (worker.getStatus() == null) ? null : worker.getStatus().getTitle());
        statement.setInt(8, worker.getOrganization().getAnnualTurnover());
        statement.setInt(9, worker.getOrganization().getEmployeesCount());
        statement.setString(10, worker.getOrganization().getType().getTitle());
        return  statement;
    }
}
