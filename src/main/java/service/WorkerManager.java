package service;

import model.*;

import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.ZonedDateTime;
import java.util.*;
import java.util.stream.Collectors;

import static java.lang.Integer.max;
import static java.lang.Integer.min;

public class WorkerManager {

    public static Worker makeWorkerFromParams(Map<String, String[]> parameters)
            throws ParseException, NumberFormatException {
        String name = parameters.get("name")[0];
        Double x = Double.parseDouble(parameters.get("coordinateX")[0]);
        double y = parameters.get("coordinateY") == null ? 0 : Double.parseDouble(parameters.get("coordinateY")[0]);
        ZonedDateTime creationDate = (parameters.get("creationDate") == null) ? ZonedDateTime.now() : //todo check if + or T
                ZonedDateTime.parse(parameters.get("creationDate")[0].replace(" ", "+"));
        Double salary = (parameters.get("salary") == null) ? null
                : Double.parseDouble(parameters.get("salary")[0]);
        Date endDate = (parameters.get("endDate") == null) ? null
                : new SimpleDateFormat("dd-MM-yyyy").parse(parameters.get("endDate")[0]);
        String position = parameters.get("position")[0];
        String status = (parameters.get("status") == null) ? "" : parameters.get("status")[0];
        Integer annualTurnover = Integer.parseInt(parameters.get("annualTurnover")[0]);
        int employeesCount = Integer.parseInt(parameters.get("employeesCount")[0]);
        String organizationType = parameters.get("organizationType")[0];

        return new Worker(name, x, y, creationDate, salary, endDate,
                position, status, annualTurnover, employeesCount, organizationType);
    }

    public static Worker updateWorkerFromParams(Map<String, String[]> parameters, Worker worker)
            throws ParseException, NumberFormatException { //todo check
        if (parameters.get("name") != null) {
            worker.setName(parameters.get("name")[0]);
        }
        if (parameters.get("coordinateX") != null) {
            worker.setCoordinateX(Double.parseDouble(parameters.get("name")[0]));
        }
        if (parameters.get("coordinateY") != null) {
            worker.setCoordinateY(parameters.get("coordinateY")[0].isEmpty() ? 0
                    : Double.parseDouble(parameters.get("coordinateY")[0]));
        }
        if (parameters.get("salary") != null) {
            worker.setSalary(Double.parseDouble(parameters.get("salary")[0]));
        }
        if (parameters.get("endDate") != null) {
            worker.setEndDate(new SimpleDateFormat("dd-MM-yyyy").parse(parameters.get("endDate")[0]));
        }
        if (parameters.get("position") != null) {
            worker.setPosition(Position.getByTitle(parameters.get("position")[0]));
        }
        if (parameters.get("status") != null) {
            worker.setStatus(Status.getByTitle(parameters.get("status")[0]));
        }
        if (parameters.get("annualTurnover") != null) {
            worker.setAnnualTurnover(Integer.parseInt(parameters.get("annualTurnover")[0]));
        }
        if (parameters.get("employeesCount") != null) {
            worker.setEmployeesCount(Integer.parseInt(parameters.get("employeesCount")[0]));
        }
        if (parameters.get("organizationType") != null) {
            worker.setOrganizationType(parameters.get("organizationType")[0]);
        }
        return worker;
    }


    public static Worker addWorker(Worker worker) throws SQLException {
        WorkerStorage.insertWorker(Objects.requireNonNull(WorkerStorage.getConnection()), worker);
        return worker;
    }

    public static Worker getWorkerById(long id) throws SQLException {
        return WorkerStorage.getWorkerById(Objects.requireNonNull(WorkerStorage.getConnection()), id);
    }

    public static Worker updateWorker(long id, Worker worker) throws SQLException {
        worker.setId(id);
        worker = WorkerStorage.updateWorker(Objects.requireNonNull(WorkerStorage.getConnection()), worker);
        return worker;
    }

    public static boolean deleteWorker(long id) throws SQLException {
        return WorkerStorage.deleteWorker(Objects.requireNonNull(WorkerStorage.getConnection()), id);
    }

    public static ArrayList<Worker> getAllWorkers() throws SQLException {
        return WorkerStorage.getAllWorkers(Objects.requireNonNull(WorkerStorage.getConnection()));
    }


    //filter fields
    //sort fields
    //paging size may be 0 (means all values)
    //page number starts with 0 and doesn't count in case page size == 0
    public static ArrayList<Worker> getWorkers(String[] filterFields, String[] filterValues, String[] sortFields,
                                               int pageSize, int pageNumber) throws SQLException {
        pageNumber = max(pageNumber, 0);
        ArrayList<Worker> workers =
                WorkerStorage.getWorkers(WorkerStorage.getConnection(), filterFields, filterValues, sortFields);
        return (pageSize == 0 || workers.size() == 0) ? workers
                : ((pageNumber * pageSize > workers.size()) ? new ArrayList<>()
                : new ArrayList<>(workers.subList(pageNumber * pageSize,
                    min((pageNumber + 1) * pageSize - 1, workers.size() - 1))));
    }

    public static Worker getWorkerWithMaxSalary() throws SQLException {
        ArrayList<Worker> workers = getAllWorkers();
        return Collections.max(workers, Comparator.comparing(Worker::getSalary));
    }

    public static long countWorkersBySalaryEqualsTo(Double salary) throws SQLException {
        return getAllWorkers().stream()
                .filter(w -> w.getSalary().equals(salary)).count();
    }

    public static ArrayList<Worker> getWorkersWithNamesStartsWith(String prefix) throws SQLException {
        return (ArrayList<Worker>) getAllWorkers().stream()
                .filter(w -> w.getName().startsWith(prefix)).collect(Collectors.toList());
    }
}
