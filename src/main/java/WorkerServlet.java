import model.Worker;
import service.WorkerManager;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;
import java.lang.reflect.Field;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.stream.Collectors;

public class WorkerServlet extends HttpServlet {

    private static final String SERVLET_PATH_WORKERS = "/workers";
    private static final String SERVLET_PATH_MAX_SALARY = "/workers/max-salary";
    private static final String SERVLET_PATH_EQUAL_SALARY = "/workers/equal-salary";
    private static final String SERVLET_PATH_NAME_STARTS_WITH = "/workers/name-starts-with";

    private static final String PAGE_NOT_FOUND = "Page not found";

    @Override
    protected void doPost(HttpServletRequest request, HttpServletResponse response) throws IOException {
        String path = request.getPathInfo();
        if (path.equals(SERVLET_PATH_WORKERS)) {
            response.setContentType("text/xml;charset=UTF-8");
            PrintWriter writer = response.getWriter();
            writer.append("<?xml version=\"1.0\" encoding=\"UTF-8\"?>");
            writer.append("<response>");
            try {
                Worker worker = WorkerManager.makeWorkerFromParams(request.getParameterMap());
                worker = WorkerManager.addWorker(worker);
                writer.append(worker.convertToXML());
                writer.append("</response>");
            } catch (SQLException e) {
                response.sendError(500, e.getMessage());
            } catch (Exception e) {
                response.sendError(422, e.getMessage());
            }
        } else {
            response.sendError(404);
        }
    }

    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws IOException {
        String path = request.getPathInfo();
        response.setContentType("text/xml;charset=UTF-8");
        PrintWriter writer = response.getWriter();
        writer.append("<?xml version=\"1.0\" encoding=\"UTF-8\"?>");
        writer.append("<response>");
        String pageSizeStr = null;
        String pageNumberStr = null;

        try {
            switch (path) {
                case SERVLET_PATH_WORKERS:
                    switch (request.getParameterMap().size()) {
                        case 0:
                                ArrayList<Worker> workers = WorkerManager.getAllWorkers();
                                writer.append(workers.stream().map(Worker::convertToXML).collect(Collectors.joining()));
                                writer.append("</response>");
                            break;

                        case 1:
                                long id = Long.parseLong(request.getParameter("id"));
                                Worker worker = WorkerManager.getWorkerById(id);
                                writer.append(worker.convertToXML());
                                writer.append("</response>");
                            break;
                        case 5:
                            pageSizeStr = request.getParameter("pageSize");
                            pageNumberStr = request.getParameter("pageNumber");
                        case 3:
                            if (request.getParameterMap().size() == 3) {
                                pageSizeStr = "0";
                                pageNumberStr = "0";
                            }
                            String filterFieldsStr = request.getParameter("filterFields");
                            String sortFieldsStr = request.getParameter("sortFields");
                            if (hasRedundantFields(filterFieldsStr) || hasRedundantFields(sortFieldsStr)) {
                                response.sendError(422, "Filter and sort parameters must contain only existing worker fields");
                            } else {
                                String[] filterFields = (filterFieldsStr.equals("")) ? new String[]{} : filterFieldsStr.split(",");
                                String[] filterValues = (request.getParameter("filterValues").equals("")) ? new String[]{} :  request.getParameter("filterValues").split(",");
                                String[] sortFields = (sortFieldsStr.equals("")) ? new String[]{} : sortFieldsStr.split(",");
                                int pageSize = Integer.parseInt(pageSizeStr);
                                int pageNumber = Integer.parseInt(pageNumberStr);
                                ArrayList<Worker> workersPage = WorkerManager.getWorkers(filterFields, filterValues,
                                        sortFields, pageSize, pageNumber);
                                writer.append(workersPage.stream().map(Worker::convertToXML).collect(Collectors.joining()));
                                writer.append("</response>");
                            }
                            break;

                        default:
                            response.sendError(400);
                    }
                    break;

                case SERVLET_PATH_MAX_SALARY:
                    if (request.getParameterMap().size() == 0) {
                        Worker worker = WorkerManager.getWorkerWithMaxSalary();
                        writer.append(worker.convertToXML());
                        writer.append("</response>");
                    } else {
                        response.sendError(422, "No parameters required");
                    }
                    break;

                case SERVLET_PATH_EQUAL_SALARY:
                    if (request.getParameterMap().size() == 1) {
                        Double salary = Double.parseDouble(request.getParameter("salary"));
                        long workersCount = WorkerManager.countWorkersBySalaryEqualsTo(salary);
                        writer.append("<count>").append(String.valueOf(workersCount)).append("</count>");
                        writer.append("</response>");
                    } else {
                        response.sendError(422, "salary parameter is required and must be the only field in this request");
                    }
                    break;

                case SERVLET_PATH_NAME_STARTS_WITH:
                    if (request.getParameterMap().size() == 1 && request.getParameter("prefix") != null) {
                        String prefix = request.getParameter("prefix");
                        ArrayList<Worker> workers = WorkerManager.getWorkersWithNamesStartsWith(prefix);
                        writer.append(workers.stream().map(Worker::convertToXML).collect(Collectors.joining()));
                        writer.append("</response>");
                    } else {
                        response.sendError(422, "prefix parameter is required and must be the only field in this request");
                    }
                    break;

                default:
                    response.sendError(404, PAGE_NOT_FOUND);
            }
        } catch (Exception e) {
            response.sendError(500, e.getMessage());
        }
    }

    @Override
    protected void doPut(HttpServletRequest request, HttpServletResponse response) throws IOException {
        String path = request.getPathInfo();
        if (path.equals(SERVLET_PATH_WORKERS)) {
            response.setContentType("text/xml;charset=UTF-8");
            PrintWriter writer = response.getWriter();
            writer.append("<?xml version=\"1.0\" encoding=\"UTF-8\"?>");
            try {
                Worker worker = WorkerManager.makeWorkerFromParams(request.getParameterMap());
                worker = WorkerManager.updateWorker(request.getParameterMap(), worker);
                writer.append("<response>");
                writer.append(worker.convertToXML());
                writer.append("</response>");
            } catch (SQLException e) {
                response.sendError(500, e.getMessage());
            } catch (Exception e) {
                response.sendError(422, e.getMessage());
            }
        } else {
            response.sendError(404);
        }
    }

    @Override
    protected void doDelete(HttpServletRequest request, HttpServletResponse response) throws IOException {
        String path = request.getPathInfo();
        if (path.equals(SERVLET_PATH_WORKERS)) {
            response.setContentType("text/xml;charset=UTF-8");
            PrintWriter writer = response.getWriter();
            writer.append("<?xml version=\"1.0\" encoding=\"UTF-8\"?>");
            if (request.getParameterMap().size() == 1) {
                try {
                    long id = Long.parseLong(request.getParameter("id"));
                    Worker worker = WorkerManager.deleteWorker(id);
                    writer.append("<response>");
                    writer.append(worker.convertToXML());
                    writer.append("</response>");
                } catch (Exception e) {
                    response.sendError(500, e.getMessage());
                }
            } else {
                response.sendError(400);
            }
        } else {
            response.sendError(404);
        }
    }

    private boolean hasRedundantFields(String fields) {
        return Arrays.stream(fields.split(","))
                .anyMatch(x -> Arrays.stream(Worker.class.getDeclaredFields())
                        .map(Field::getName)
                        .noneMatch(x::equals)) && !fields.isEmpty();
    }
}
