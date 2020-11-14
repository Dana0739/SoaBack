import model.OrganizationType;
import model.Position;
import model.Status;
import model.Worker;
import service.WorkerManager;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.ZonedDateTime;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class WorkerServlet extends HttpServlet {

    private static final String SERVLET_PATH_WORKERS = "/workers";
    private static final String SERVLET_PATH_MAX_SALARY = "/workers/max-salary";
    private static final String SERVLET_PATH_EQUAL_SALARY = "/workers/equal-salary";
    private static final String SERVLET_PATH_NAME_STARTS_WITH = "/workers/name-starts-with";

    private static final ArrayList<String> WORKER_FIELDS = new ArrayList<>(Arrays.asList("name","coordinateX",
            "coordinateY","salary","endDate","position","status","annualTurnover","employeesCount","organizationType"));
    private static final ArrayList<String> WORKER_FIELDS_WITH_ID_AND_CREATION_DATE =
            new ArrayList<>(Arrays.asList("name","coordinateX", "coordinateY","salary","endDate","position","status",
                    "annualTurnover","employeesCount","organizationType","id","creationDate"));

    @Override
    protected void doPost(HttpServletRequest request, HttpServletResponse response) throws IOException {
        String path = request.getPathInfo();
        if (path.equals(SERVLET_PATH_WORKERS)) {
            response.setContentType("text/xml;charset=UTF-8");
            PrintWriter writer = response.getWriter();
            writer.append("<?xml version=\"1.0\" encoding=\"UTF-8\"?>");
            writer.append("<response>");
            try {
                if (hasRedundantParameters(request.getParameterMap().keySet())) {
                    response.sendError(422);
                } else {
                    Worker worker = WorkerManager.makeWorkerFromParams(request.getParameterMap());
                    worker = WorkerManager.addWorker(worker);
                    writer.append(worker.convertToXML());
                    writer.append("</response>");
                }
            } catch (NumberFormatException | ParseException e) {
                System.out.println(e.getMessage());
                response.sendError(422, e.getMessage());
            } catch (SQLException e) {
                System.out.println(e.getMessage());
                response.sendError(500, e.getMessage());
            }
        } else if (checkUrlWithRegExp(path)
                || path.equals(SERVLET_PATH_EQUAL_SALARY)
                || path.equals(SERVLET_PATH_MAX_SALARY)
                || path.equals(SERVLET_PATH_NAME_STARTS_WITH)) {
            response.sendError(405);
        } else {
            response.sendError(400);
        }
    }

    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws IOException {
        String path = request.getPathInfo();
        response.setContentType("text/xml;charset=UTF-8");
        PrintWriter writer = response.getWriter();
        writer.append("<?xml version=\"1.0\" encoding=\"UTF-8\"?>");
        writer.append("<response>");

        try {
            if (path.equals(SERVLET_PATH_WORKERS)) {
                if (request.getParameterMap().size() == 0) {  // https://{server-app}/workers
                    ArrayList<Worker> workers = WorkerManager.getAllWorkers();
                    if (workers.isEmpty()) {
                        response.sendError(404);
                    } else {
                        writer.append(workers.stream().map(Worker::convertToXML).collect(Collectors.joining()));
                    }
                    writer.append("</response>");
                } else {  // https://{server-app}/workers?arg1=1&arg2=2...
                    if (checkParametersForFilterSort(request.getParameterMap())) {
                        String pageSizeStr = request.getParameter("pageSize");
                        String pageNumberStr = request.getParameter("pageNumber");
                        int pageSize = (pageSizeStr == null || pageSizeStr.isEmpty())
                                ? 0 : Integer.parseInt(pageSizeStr);
                        int pageNumber = (pageNumberStr == null || pageNumberStr.isEmpty())
                                ? 0 : Integer.parseInt(pageNumberStr);
                        String filterFieldsStr = request.getParameter("filterFields");
                        String sortFieldsStr = request.getParameter("sortFields");
                        String[] filterFields = (filterFieldsStr == null || filterFieldsStr.isEmpty())
                                ? new String[]{} : filterFieldsStr.split(",");
                        String[] filterValues = (request.getParameter("filterValues") == null
                                || request.getParameter("filterValues").isEmpty()) ? new String[]{}
                                : request.getParameter("filterValues").split(",");
                        String[] sortFields = (sortFieldsStr == null || sortFieldsStr.isEmpty()) ? new String[]{} :
                                sortFieldsStr.split(",");
                        ArrayList<Worker> workersPage = WorkerManager.getWorkers(filterFields, filterValues,
                                sortFields, pageSize, pageNumber);
                        if (workersPage.isEmpty()) {
                            response.sendError(404); // no content
                        } else {
                            writer.append(workersPage.stream().map(Worker::convertToXML).collect(Collectors.joining()));
                        }
                        writer.append("</response>");
                    } else {
                        response.sendError(422); // unprocessable entity
                    }
                }
            } else if (checkUrlWithRegExp(path)) { // https://{server-app}/workers/1
                if (request.getParameterMap().size() == 0) {
                    long id = Long.parseLong(path.substring(path.lastIndexOf(SERVLET_PATH_WORKERS)
                            + SERVLET_PATH_WORKERS.length() + 1));
                    Worker worker = WorkerManager.getWorkerById(id);
                    if (worker == null) {
                        response.sendError(404); // no content
                    } else {
                        writer.append(worker.convertToXML());
                    }
                    writer.append("</response>");
                } else {
                    response.sendError(400); // bad request
                }
            } else if (path.equals(SERVLET_PATH_MAX_SALARY)) { // https://{server-app}/workers/max-salary
                if (request.getParameterMap().size() == 0) {
                    Worker worker = WorkerManager.getWorkerWithMaxSalary();
                    writer.append(worker.convertToXML());
                    writer.append("</response>");
                } else {
                    response.sendError(400); // bad request
                }
            } else if (path.equals(SERVLET_PATH_EQUAL_SALARY)) { // https://{server-app}/workers/equal-salary?salary=1
                if (request.getParameterMap().size() == 1 && request.getParameter("salary") != null) {
                    try {
                        Double salary = Double.parseDouble(request.getParameter("salary"));
                        long workersCount = WorkerManager.countWorkersBySalaryEqualsTo(salary);
                        writer.append("<count>").append(String.valueOf(workersCount)).append("</count>");
                        writer.append("</response>");
                    } catch (NumberFormatException e) {
                        System.out.println(e.getMessage());
                        response.sendError(422, e.getMessage()); // unprocessable entity
                    }
                } else {
                    response.sendError(422); // unprocessable entity
                }
            } else if (path.equals(SERVLET_PATH_NAME_STARTS_WITH)) { // https://{server-app}/workers/name-starts-with?prefix=Dana
                if (request.getParameterMap().size() == 1 && request.getParameter("prefix") != null) {
                    String prefix = request.getParameter("prefix");
                    ArrayList<Worker> workers = WorkerManager.getWorkersWithNamesStartsWith(prefix);
                    if (workers.isEmpty()) {
                        response.sendError(404); // no content
                    } else {
                        writer.append(workers.stream().map(Worker::convertToXML).collect(Collectors.joining()));
                    }
                    writer.append("</response>");
                } else {
                    response.sendError(422); // unprocessable entity
                }
            } else {
                response.sendError(400); // bad request
            }
        } catch (Exception e) {
            System.out.println(e.getMessage());
            response.sendError(500, e.getMessage());
        }
    }

    @Override
    protected void doPut(HttpServletRequest request, HttpServletResponse response) throws IOException {
        String path = request.getPathInfo();
        if (checkUrlWithRegExp(path)) {
            response.setContentType("text/xml;charset=UTF-8");
            PrintWriter writer = response.getWriter();
            writer.append("<?xml version=\"1.0\" encoding=\"UTF-8\"?>");
            try {
                if (hasRedundantParameters(request.getParameterMap().keySet())) {
                    response.sendError(422);
                } else {
                    long id = Long.parseLong(path.substring(path.lastIndexOf(SERVLET_PATH_WORKERS)
                            + SERVLET_PATH_WORKERS.length() + 1));
                    Worker worker = WorkerManager.getWorkerById(id);
                    worker = WorkerManager.updateWorkerFromParams(request.getParameterMap(), worker);
                    worker = WorkerManager.updateWorker(id, worker);
                    writer.append("<response>");
                    writer.append(worker.convertToXML());
                    writer.append("</response>");
                }
            } catch (NumberFormatException | ParseException e) {
                System.out.println(e.getMessage());
                response.sendError(422, e.getMessage());
            } catch (Exception e) {
                System.out.println(e.getMessage());
                response.sendError(500, e.getMessage());
            }
        } else if (path.equals(SERVLET_PATH_WORKERS)
                || path.equals(SERVLET_PATH_EQUAL_SALARY)
                || path.equals(SERVLET_PATH_MAX_SALARY)
                || path.equals(SERVLET_PATH_NAME_STARTS_WITH)) {
            response.sendError(405);
        } else {
            response.sendError(400);
        }
    }

    @Override
    protected void doDelete(HttpServletRequest request, HttpServletResponse response) throws IOException {
        String path = request.getPathInfo();
        if (checkUrlWithRegExp(path)) {
            if (request.getParameterMap().size() == 0) {
                try {
                    long id = Long.parseLong(path.substring(path.lastIndexOf(SERVLET_PATH_WORKERS)
                            + SERVLET_PATH_WORKERS.length() + 1));
                    if (!WorkerManager.deleteWorker(id)) response.sendError(500);
                } catch (NumberFormatException e) {
                    System.out.println(e.getMessage());
                    response.sendError(422, e.getMessage());
                } catch (SQLException e) {
                    System.out.println(e.getMessage());
                    response.sendError(500, e.getMessage());
                }
            } else {
                response.sendError(400);
            }
        } else if (path.equals(SERVLET_PATH_WORKERS)
                || path.equals(SERVLET_PATH_EQUAL_SALARY)
                || path.equals(SERVLET_PATH_MAX_SALARY)
                || path.equals(SERVLET_PATH_NAME_STARTS_WITH)) {
            response.sendError(405);
        } else {
            response.sendError(400);
        }
    }

    private static boolean checkUrlWithRegExp(String url){
        Pattern p = Pattern.compile("^" + SERVLET_PATH_WORKERS + "/[0-9]*$");
        Matcher m = p.matcher(url);
        return m.matches();
    }

    private static boolean hasRedundantParameters(Set<String> params) {
        return params.stream().anyMatch(x -> WORKER_FIELDS.stream()
                        .noneMatch(x::equals));
    }

    private static boolean hasRedundantFields(String fields) {
        return Arrays.stream(fields.split(","))
                .anyMatch(x -> WORKER_FIELDS_WITH_ID_AND_CREATION_DATE.stream()
                        .noneMatch(x::equals)) && !fields.isEmpty();
    }

    private static boolean checkFilterValuesForFilterSort(String[] filterFields, String[] filterValues) {
        try {
            for (int i = 0; i < filterFields.length; ++i) {
                switch (filterFields[i]) {
                    case "position":
                        if (Position.getByTitle(filterValues[i]) == null) return false;
                        break;
                    case "status":
                        if (Status.getByTitle(filterValues[i]) == null) return false;
                        break;
                    case "organizationType":
                        if (OrganizationType.getByTitle(filterValues[i]) == null) return false;
                        break;
                    case "id":
                    case "employeesCount":
                    case "annualTurnover":
                        int inumber = Integer.parseInt(filterValues[i]);
                        if (inumber < 0) return false;
                        break;
                    case "salary": //double >= 0 or null
                        if (!filterValues[i].equals("null")) {
                            double dnumber = Double.parseDouble(filterValues[i]);
                            if (dnumber < 0) return false;
                        }
                        break;
                    case "coordinateX":
                    case "coordinateY":
                        double dnumber = Double.parseDouble(filterValues[i]);
                        if (filterFields[i].equals("coordinateY") && dnumber > 444) return false;
                        break;
                    case "endDate":
                        Date endDate = (filterValues[i].equals("null")) ? null
                                : new SimpleDateFormat("dd-MM-yyyy").parse(filterValues[i]);
                        break;
                    case "creationDate":
                        ZonedDateTime creationDate = ZonedDateTime.parse(filterValues[i].replace(" ", "+"));
                        break;
                    case "name":
                        break;
                    default:
                        return false;
                }
            }
        } catch (Exception e) {
            return false;
        }
        return true;
    }

    private static boolean checkFilterForFilterSort(String filterFields, String filterValues) {
        return !hasRedundantFields(filterFields) &&
                filterFields.split(",").length == filterValues.split(",").length &&
                checkFilterValuesForFilterSort(filterFields.split(","), filterValues.split(","));
    }

    private static boolean checkParametersForFilterSort(Map<String, String[]> params) {
        try {
            int counter = 0;
            if (params.containsKey("pageSize")) ++counter;
            if (params.containsKey("pageNumber")) ++counter;
            if (params.containsKey("filterFields") && params.containsKey("filterValues")) counter += 2;
            if (params.containsKey("sortFields")) ++counter;
            return counter == params.size() &&
                    (params.get("pageSize") == null || Integer.parseInt(params.get("pageSize")[0]) >= 0) &&
                    (params.get("pageNumber") == null || Integer.parseInt(params.get("pageNumber")[0]) >= 0) &&
                    (!params.containsKey("filterFields") && !params.containsKey("filterValues")
                            || checkFilterForFilterSort(params.get("filterFields")[0], params.get("filterValues")[0]));
        } catch (NumberFormatException e) {
            return false;
        }
    }
}
