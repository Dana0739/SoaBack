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
    static final String USER = "s243875";
    static final String PASS = "xic778";

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
        List<String> filterFieldsAL = Arrays.asList(filterFields);
        Map<String, Integer> filterFieldsMap = new HashMap<>();
        List<String> filterFieldsList = new ArrayList<>();
        int i = 0;
        for (String key : filterFieldsAL) {
            filterFieldsMap.put(FIELDS.get(key), i);
            filterFieldsList.add(FIELDS.get(key));
            i++;
        }
        StringBuilder selectBuilder = new StringBuilder("SELECT * FROM WORKER WHERE ");
        for (String field: filterFields) {
            selectBuilder.append(FIELDS.get(field));
            selectBuilder.append(" = ? and ");
        }
        selectBuilder.replace(selectBuilder.lastIndexOf("and"), selectBuilder.lastIndexOf("and") + 3, "");
        selectBuilder.append("ORDER BY ");
        for (String field: sortFields) {
            selectBuilder.append(FIELDS.get(field));
            selectBuilder.append(", ");
        }
        selectBuilder.deleteCharAt(selectBuilder.lastIndexOf(","));
        selectBuilder.append(";");
        PreparedStatement preparedStatement = connection.prepareStatement(selectBuilder.toString());

        if (filterFieldsList.contains("ID")) {
            preparedStatement.setLong(filterFieldsMap.get("ID") + 1,
                    Long.parseLong(filterValues[filterFieldsMap.get("ID")]));
        }
        if (filterFieldsList.contains("NAME")) {
            preparedStatement.setString(filterFieldsMap.get("NAME") + 1,
                    filterValues[filterFieldsMap.get("NAME")]);
        }
        if (filterFieldsList.contains("COORDINATE_X")) {
            preparedStatement.setDouble(filterFieldsMap.get("COORDINATE_X") + 1,
                    Double.parseDouble(filterValues[filterFieldsMap.get("COORDINATE_X")]));
        }
        if (filterFieldsList.contains("COORDINATE_Y")) {
            preparedStatement.setDouble(filterFieldsMap.get("COORDINATE_Y") + 1,
                    Double.parseDouble(filterValues[filterFieldsMap.get("COORDINATE_Y")]));
        }
        if (filterFieldsList.contains("CREATION_DATE")) {
            preparedStatement.setTimestamp(filterFieldsMap.get("CREATION_DATE") + 1,
                    Timestamp.valueOf(filterValues[filterFieldsMap.get("CREATION_DATE")]));
        }
        if (filterFieldsList.contains("SALARY")) {
            preparedStatement.setDouble(filterFieldsMap.get("SALARY") + 1,
                    Double.parseDouble(filterValues[filterFieldsMap.get("SALARY")]));
        }
        if (filterFieldsList.contains("END_DATE")) {
            preparedStatement.setTimestamp(filterFieldsMap.get("END_DATE") + 1,
                    Timestamp.valueOf(filterValues[filterFieldsMap.get("END_DATE")]));
        }
        if (filterFieldsList.contains("POSITION")) {
            preparedStatement.setString(filterFieldsMap.get("POSITION") + 1,
                    filterValues[filterFieldsMap.get("POSITION")]);
        }
        if (filterFieldsList.contains("STATUS")) {
            preparedStatement.setString(filterFieldsMap.get("STATUS") + 1,
                    filterValues[filterFieldsMap.get("STATUS")]);
        }
        if (filterFieldsList.contains("O_ANNUAL_TURNOVER")) {
            preparedStatement.setInt(filterFieldsMap.get("O_ANNUAL_TURNOVER") + 1,
                    Integer.parseInt(filterValues[filterFieldsMap.get("O_ANNUAL_TURNOVER")]));
        }
        if (filterFieldsList.contains("O_EMPLOYEES_COUNT")) {
            preparedStatement.setInt(filterFieldsMap.get("O_EMPLOYEES_COUNT") + 1,
                    Integer.parseInt(filterValues[filterFieldsMap.get("O_EMPLOYEES_COUNT")]));
        }
        if (filterFieldsList.contains("O_ORGANIZATION_TYPE")) {
            preparedStatement.setString(filterFieldsMap.get("O_ORGANIZATION_TYPE") + 1,
                    filterValues[filterFieldsMap.get("O_ORGANIZATION_TYPE")]);
        }

        ResultSet rs = preparedStatement.executeQuery();
        ArrayList<Worker> workers = collectWorkersFromRs(rs);
        rs.close();
        connection.close();
        return workers;
    }

    public static ArrayList<Worker> getAllWorkers(Connection connection) throws SQLException {
        Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery( "SELECT * FROM WORKER;");
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

    public static Worker deleteWorker(Connection connection, long id) throws SQLException {
        Worker worker = getWorkerByIdSaveConnection(connection, id);
        PreparedStatement delete = connection.prepareStatement("DELETE FROM WORKER WHERE ID = ?;");
        delete.setLong(1, id);
        delete.executeUpdate();
        delete.close();
        connection.close();
        return worker;
    }

    private static Worker makeWorkerFromRs(ResultSet rs) throws SQLException {
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
        statement.setDouble(4, worker.getSalary());
        statement.setTimestamp(5, new Timestamp(worker.getEndDate().getTime()));
        statement.setString(6, worker.getPosition().getTitle());
        statement.setString(7, worker.getStatus().getTitle());
        statement.setInt(8, worker.getOrganization().getAnnualTurnover());
        statement.setInt(9, worker.getOrganization().getEmployeesCount());
        statement.setString(10, worker.getOrganization().getType().getTitle());
        return  statement;
    }
}
