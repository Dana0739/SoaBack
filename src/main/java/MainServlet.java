import model.Worker;
import service.CRUDService;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;
import java.lang.reflect.Field;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Objects;
import java.util.stream.Collectors;

public class MainServlet extends HttpServlet {

    private static final String SERLVET_PATH_MAIN = "/Main";
    private static final String SERLVET_PATH_MAX_SALARY = "/MaxSalary";
    private static final String SERLVET_PATH_EQUAL_SALARY = "/EqualSalary";
    private static final String SERLVET_PATH_NAME_STARTS_WITH = "/NameStartsWith";

    private static final String GET_BY_ID = "getById";
    private static final String GET_ALL_WORKERS = "getAllWorkers";
    private static final String GET_WORKERS_FILTER_SORT = "getWorkersFilterSort";

    private static final String PAGE_NOT_FOUND = "Page not found";

    @Override
    protected void doPost(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {
        response.setContentType("text/xml;charset=UTF-8");
        PrintWriter writer = response.getWriter();
        writer.append("<?xml version=\"1.0\" encoding=\"UTF-8\"?>");
        writer.append("<response>");
        try {
            Worker worker = CRUDService.addWorker(request.getParameterMap());
            writer.append(worker.convertToXML());
            writer.append("</response>");
        } catch (Exception e) {
            response.sendError(400, e.getMessage());
        }
    }

    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {
        String path = request.getPathInfo();
        response.setContentType("text/xml;charset=UTF-8");
        PrintWriter writer = response.getWriter();
        writer.append("<?xml version=\"1.0\" encoding=\"UTF-8\"?>");
        writer.append("<response>");

        try {
            switch (path) {
                case SERLVET_PATH_MAIN:
                    String requestedFunction = request.getParameter("function");
                    if (requestedFunction == null) {
                        response.sendError(422, "Parameter \"function\" is required");
                    } else {
                        switch (requestedFunction) {

                            case GET_BY_ID:
                                if (request.getParameterMap().size() == 2) {
                                    long id = Long.parseLong(request.getParameter("id"));
                                    Worker worker = CRUDService.getWorkerById(id);
                                    writer.append(worker.convertToXML());
                                    writer.append("</response>");
                                } else {
                                    response.sendError(422, "id parameter must be the only field in this request");
                                }
                                break;

                            case GET_ALL_WORKERS:
                                if (request.getParameterMap().size() == 1) {
                                    ArrayList<Worker> workers = CRUDService.getAllWorkers();
                                    writer.append(workers.stream().map(Worker::convertToXML).collect(Collectors.joining()));
                                    writer.append("</response>");
                                } else {
                                    response.sendError(422, "No parameters except function required");
                                }
                                break;

                            case GET_WORKERS_FILTER_SORT:
                                String filterFieldsStr = request.getParameter("filterFields");
                                String sortFieldsStr = request.getParameter("sortFields");
                                if (hasRedundantFields(filterFieldsStr) || hasRedundantFields(sortFieldsStr)) {
                                    response.sendError(422, "Filter and sort parameters must contain only existing worker fields");
                                } else {
                                    String[] filterFields = filterFieldsStr.split(",");
                                    String[] filterValues = request.getParameter("filterValues").split(",");
                                    String[] sortFields = sortFieldsStr.split(",");
                                    String pageSizeStr = Objects.requireNonNullElse(request.getParameter("pageSize"), "0");
                                    String pageNumberStr = Objects.requireNonNullElse(request.getParameter("pageNumber"), "0");
                                    int pageSize = Integer.parseInt(pageSizeStr);
                                    int pageNumber = Integer.parseInt(pageNumberStr);
                                    ArrayList<Worker> workersPage = CRUDService.getWorkers(filterFields, filterValues,
                                            sortFields, pageSize, pageNumber);
                                    writer.append(workersPage.stream().map(Worker::convertToXML).collect(Collectors.joining()));
                                    writer.append("</response>");
                                }
                                break;

                            default:
                                response.sendError(400, "There must be parameter \"function\" having value " +
                                        "getById, getAllWorkers or getWorkersFilterSort");
                        }
                    }
                    break;

                case SERLVET_PATH_MAX_SALARY:
                    if (request.getParameterMap().size() == 0) {
                        Worker worker = CRUDService.getWorkerWithMaxSalary();
                        writer.append(worker.convertToXML());
                        writer.append("</response>");
                    } else {
                        response.sendError(422, "No parameters required");
                    }
                    break;

                case SERLVET_PATH_EQUAL_SALARY:
                    if (request.getParameterMap().size() == 1) {
                        Double salary = Double.parseDouble(request.getParameter("salary"));
                        ArrayList<Worker> workers = CRUDService.countWorkersBySalaryEqualsTo(salary);
                        writer.append(workers.stream().map(Worker::convertToXML).collect(Collectors.joining()));
                        writer.append("</response>");
                    } else if (request.getParameterMap().size() == 0) {
                        response.sendError(422, "salary parameter is required");
                    } else {
                        response.sendError(422, "salary parameter must be the only field in this request");
                    }
                    break;

                case SERLVET_PATH_NAME_STARTS_WITH:
                    if (request.getParameterMap().size() == 1 && request.getParameter("prefix") != null) {
                        String prefix = request.getParameter("prefix");
                        ArrayList<Worker> workers = CRUDService.getWorkersWithNamesStartsWith(prefix);
                        writer.append(workers.stream().map(Worker::convertToXML).collect(Collectors.joining()));
                        writer.append("</response>");
                    } else {
                        response.sendError(422, "prefix parameter is required and must be the only field in this request");
                    }
                    break;

                default:
                    response.sendError(404, PAGE_NOT_FOUND);

            }
        } catch (SQLException e) {
            response.sendError(500, e.getMessage());
        }
    }

    @Override
    protected void doPut(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {
        response.setContentType("text/xml;charset=UTF-8");
        PrintWriter writer = response.getWriter();
        writer.append("<?xml version=\"1.0\" encoding=\"UTF-8\"?>");
        try {
            Worker worker = CRUDService.updateWorker(request.getParameterMap());
            writer.append("<response>");
            writer.append(worker.convertToXML());
            writer.append("</response>");
        } catch (Exception e) {
            response.sendError(400, e.getMessage());
        }
    }

    @Override
    protected void doDelete(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {
        response.setContentType("text/xml;charset=UTF-8");
        PrintWriter writer = response.getWriter();
        writer.append("<?xml version=\"1.0\" encoding=\"UTF-8\"?>");

        if (request.getParameterMap().size() > 1) {
            response.sendError(422, "id parameter must be the only field in this request");
        } else if (request.getParameterMap().size() == 0) {
            response.sendError(422, "id parameter is required");
        } else {
            try {
                long id = Long.parseLong(request.getParameter("id"));
                Worker worker = CRUDService.deleteWorker(id);
                writer.append("<response>");
                writer.append(worker.convertToXML());
                writer.append("</response>");
            } catch (NumberFormatException e) {
                response.sendError(422, "id parameter must be numeric value");
            } catch (Exception e) {
                response.sendError(400, e.getMessage());
            }
        }
    }

    private boolean hasRedundantFields(String fields) {
        return Arrays.stream(fields.split(","))
                .anyMatch(x -> Arrays.stream(Worker.class.getDeclaredFields())
                        .map(Field::getName)
                        .noneMatch(x::equals));
    }
}
