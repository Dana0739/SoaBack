package service;

import model.*;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.*;
import java.util.stream.Collectors;

public class CRUDService {

    public static Worker addWorker(Map<String, String[]> parameters) throws ParseException, SQLException {
        String name = parameters.get("name")[0];
        Double x = Double.parseDouble(parameters.get("coordinateX")[0]);
        double y = parameters.get("coordinateY") == null ? 0 : Double.parseDouble(parameters.get("coordinateY")[0]);
        ZonedDateTime creationDate = ZonedDateTime.now();
        Double salary = Double.parseDouble(parameters.get("salary")[0]);
        Date endDate = parameters.get("endDate") == null ? null : new SimpleDateFormat("dd/MM/yyyy").parse(parameters.get("endDate")[0]);
        String position = parameters.get("position")[0];
        String status = parameters.get("status")[0];
        Integer annualTurnover = Integer.parseInt(parameters.get("annualTurnover")[0]);
        int employeesCount = Integer.parseInt(parameters.get("employeesCount")[0]);
        String organizationType = parameters.get("organizationType")[0];

        Worker worker = new Worker(name, x, y, creationDate, salary, endDate,
                position, status, annualTurnover, employeesCount, organizationType);
        DBService.insertWorker(DBService.getConnection(), worker);
        return worker;
    }

    public static Worker getWorkerById(long id) throws SQLException {
        return DBService.getWorkerById(DBService.getConnection(), id);
    }

    public static Worker updateWorker(Map<String, String[]> parameters) throws ParseException, SQLException {
        long id = Long.parseLong(parameters.get("id")[0]);
        String name = parameters.get("name")[0];
        Double x = Double.parseDouble(parameters.get("coordinateX")[0]);
        double y = parameters.get("coordinateY") == null ? 0 : Double.parseDouble(parameters.get("coordinateY")[0]);
        ZonedDateTime creationDate = ZonedDateTime.parse(parameters.get("creationDate")[0].replace(" ", "+"));
        Double salary = Double.parseDouble(parameters.get("salary")[0]);
        Date endDate = parameters.get("endDate") == null ? null : new SimpleDateFormat("dd/MM/yyyy").parse(parameters.get("endDate")[0]);
        String position = parameters.get("position")[0];
        String status = parameters.get("status")[0];
        Integer annualTurnover = Integer.parseInt(parameters.get("annualTurnover")[0]);
        int employeesCount = Integer.parseInt(parameters.get("employeesCount")[0]);
        String organizationType = parameters.get("organizationType")[0];

        Worker worker = new Worker(name, x, y, creationDate, salary, endDate,
                position, status, annualTurnover, employeesCount, organizationType).setId(id);
        DBService.updateWorker(DBService.getConnection(), worker);
        return worker;
    }

    public static Worker deleteWorker(long id) throws SQLException {
        return DBService.deleteWorker(DBService.getConnection(), id);
    }

    public static ArrayList<Worker> getAllWorkers() throws SQLException {
        return DBService.getAllWorkers(DBService.getConnection());
    }


    //filter fields
    //sort fields
    //paging size may be 0
    //page number may be 0
    public static ArrayList<Worker> getWorkers(String[] filterFields, String[] filterValues,
                                        String[] sortFields, int pageSize, int pageNumber) throws SQLException {
        return DBService.getWorkers(DBService.getConnection(), filterFields, filterValues, sortFields, pageSize, pageNumber);
    }

    public static Worker getWorkerWithMaxSalary() throws SQLException {
        ArrayList<Worker> workers = getAllWorkers();
        return Collections.max(workers, Comparator.comparing(Worker::getSalary));
    }

    public static ArrayList<Worker> countWorkersBySalaryEqualsTo(Double salary) throws SQLException {
        return (ArrayList<Worker>) getAllWorkers().stream()
                .filter(w -> w.getSalary().equals(salary)).collect(Collectors.toList());
    }

    public static ArrayList<Worker> getWorkersWithNamesStartsWith(String prefix) throws SQLException {
        return (ArrayList<Worker>) getAllWorkers().stream()
                .filter(w -> w.getName().startsWith(prefix)).collect(Collectors.toList());
    }
}
