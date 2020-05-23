package corona;


import corona.business.Employee;
import corona.business.Lab;
import corona.business.ReturnValue;
import corona.business.Vaccine;
import corona.data.DBConnector;
import corona.data.PostgreSQLErrorCodes;
import static corona.business.ReturnValue.*;
import static corona.data.PostgreSQLErrorCodes.*;

import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;


public class Solution {
    private static final String CREATE_TABLE_LABS =
            "CREATE TABLE Labs\n" +
            "(\n" +
            "    id integer NOT NULL,\n" +
            "    name text NOT NULL,\n" +
            "    city text NOT NULL,\n" +
            "    isActive boolean NOT NULL,\n" +
            "    PRIMARY KEY (id),\n" +
            "    CHECK (id > 0)\n" +
            ")";
    private static final String CREATE_TABLE_EMPLOYEES =
            "CREATE TABLE Employees\n" +
                    "(\n" +
                    "    id integer NOT NULL,\n" +
                    "    name text NOT NULL,\n" +
                    "    cityOfBirth text NOT NULL,\n" +
                    "    PRIMARY KEY (id),\n" +
                    "    CHECK (id > 0)\n" +
                    ")";
    private static final String CREATE_TABLE_VACCINES =
            "CREATE TABLE Vaccines\n" +
                    "(\n" +
                    "    id integer NOT NULL,\n" +
                    "    name text NOT NULL,\n" +
                    "    cost integer NOT NULL,\n" +
                    "    unitsInStock integer NOT NULL,\n" +
                    "    productivity integer NOT NULL,\n" +
                    "    PRIMARY KEY (id),\n" +
                    "    CHECK (id > 0),\n" +
                    "    CHECK (cost >= 0),\n" +
                    "    CHECK (unitsInStock >= 0),\n" +
                    "    CHECK (productivity >= 0)\n" +
                    ")";

    public static void createTables() {
        //TODO add tables
        List<String> queries = Arrays.asList(CREATE_TABLE_LABS, CREATE_TABLE_EMPLOYEES, CREATE_TABLE_VACCINES);
        createQueriesAndConnection(queries);
    }

    /**
     * Clears all of the tables in the DB.
     */
    public static void clearTables() {
        // TODO add tables

        List<String> tables = Arrays.asList("LABS", "EMPLOYEES", "VACCINES");
        tables = tables.stream().map(s -> String.format("DELETE FROM %s", s)).collect(Collectors.toList());
        createQueriesAndConnection(tables);
    }

    /**
     * Creates the queries to drop the tables, opens a connection and drops them.
     */
    public static void dropTables() {
        // TODO add drop to views
        // TODO CASCADE?

        List<String> tables = Arrays.asList("LABS", "EMPLOYEES", "VACCINES");
        tables = tables.stream().map(s -> String.format("DROP TABLE IF EXISTS %s", s)).collect(Collectors.toList());
        createQueriesAndConnection(tables);
    }

    public static ReturnValue addLab(Lab lab) {
        if (null == lab) {
            return BAD_PARAMS;
        }

        ReturnValue retVal = OK;
        String query = "INSERT INTO Labs \n" +
                        "VALUES (?, ?, ?, ?)";
        PreparedStatement pstmt = null;
        Connection connection = DBConnector.getConnection();

        if (null != connection) {
            try {
                pstmt = setAddLabParams(connection, query, lab);
                pstmt.execute();
            } catch (SQLException e) {
                handleException(e);
                retVal = getError(e);
            } finally {
                closeAll(connection, pstmt); //TODO necessary?
            }
        }

        return retVal;
    }

    public static Lab getLabProfile(Integer labID) {
        Connection connection = DBConnector.getConnection();
        ResultSet res = null;
        if (null == connection || null == labID) return Lab.badLab();

        PreparedStatement pstmt = null;
        Lab lab = new Lab();
        try {
            pstmt = connection.prepareStatement("SELECT * FROM Labs WHERE id = ?");
            pstmt.setInt(1, labID);
            res = pstmt.executeQuery();
            lab.setId(labID);
            lab.setName(res.getString("name"));
            lab.setCity(res.getString("city"));
            lab.setIsActive(res.getBoolean("isActive"));
            return lab;

        } catch (SQLException e) {
            handleException(e);
            lab = Lab.badLab();
        } finally {
            closeAll(connection, pstmt, res);
        }

        return lab;
    }

    public static ReturnValue deleteLab(Lab lab) {
        return removeEntryFromTable("Labs", lab.getId());
    }

    public static ReturnValue addEmployee(Employee employee) {
        if (null == employee) {
            return BAD_PARAMS;
        }

        ReturnValue retVal = OK;
        String query = "INSERT INTO Employees \n" +
                "VALUES (?, ?, ?)";
        PreparedStatement pstmt = null;
        Connection connection = DBConnector.getConnection();

        if (null != connection) {
            try {
                pstmt = setAddEmployeeParams(connection, query, employee);
                pstmt.execute();
            } catch (SQLException e) {
                handleException(e);
                retVal = getError(e);
            } finally {
                closeAll(connection, pstmt); //TODO necessary?
            }
        }

        return retVal;
    }

    public static Employee getEmployeeProfile(Integer employeeID) {
        Connection connection = DBConnector.getConnection();
        ResultSet res = null;
        if (null == connection || null == employeeID) return Employee.badEmployee();

        PreparedStatement pstmt = null;
        Employee employee = new Employee();
        try {
            pstmt = connection.prepareStatement("SELECT * FROM Employees WHERE id = ?");
            pstmt.setInt(1, employeeID);
            res = pstmt.executeQuery();
            employee.setId(employeeID);
            employee.setName(res.getString("name"));
            employee.setCity(res.getString("cityOfBirth"));
            return employee;

        } catch (SQLException e) {
            handleException(e);
            employee = Employee.badEmployee();
        } finally {
            closeAll(connection, pstmt, res);
        }

        return employee;
    }

    public static ReturnValue deleteEmployee(Employee employee) {
        return removeEntryFromTable("Employees", employee.getId());
    }

    public static ReturnValue addVaccine(Vaccine vaccine) {
        if (null == vaccine) {
            return BAD_PARAMS;
        }

        ReturnValue retVal = OK;
        String query = "INSERT INTO Vaccines \n" +
                "VALUES (?, ?, ?, ?, ?)"; //TODO update if vaccine is updated
        PreparedStatement pstmt = null;
        Connection connection = DBConnector.getConnection();

        if (null != connection) {
            try {
                pstmt = setAddVaccineParams(connection, query, vaccine);
                pstmt.execute();
            } catch (SQLException e) {
                handleException(e);
                retVal = getError(e);
            } finally {
                closeAll(connection, pstmt); //TODO necessary?
            }
        }

        return retVal;
    }

    public static Vaccine getVaccineProfile(Integer vaccineID) {
        Connection connection = DBConnector.getConnection();
        ResultSet res = null;
        if (null == connection || null == vaccineID) return Vaccine.badVaccine();

        PreparedStatement pstmt = null;
        Vaccine vaccine = new Vaccine();
        try {
            pstmt = connection.prepareStatement("SELECT * FROM Vaccines WHERE id = ?");
            pstmt.setInt(1, vaccineID);
            res = pstmt.executeQuery();
            vaccine.setId(vaccineID);
            vaccine.setName(res.getString("name"));
            vaccine.setCost(res.getInt("cost"));
            vaccine.setUnits(res.getInt("unitsInStock"));
            vaccine.setProductivity(res.getInt("productivity"));
            return vaccine;

        } catch (SQLException e) {
            handleException(e);
            vaccine = Vaccine.badVaccine();
        } finally {
            closeAll(connection, pstmt, res);
        }

        return vaccine;
    }

    public static ReturnValue deleteVaccine(Vaccine vaccine) {
        return OK;
    }


    public static ReturnValue employeeJoinLab(Integer employeeID, Integer labID, Integer salary) {
        return OK;
    }

    public static ReturnValue employeeLeftLab(Integer labID, Integer employeeID) {
        return OK;
    }

    public static ReturnValue labProduceVaccine(Integer vaccineID, Integer labID) {
        return OK;
    }

    public static ReturnValue labStoppedProducingVaccine(Integer labID, Integer vaccineID) {
        return OK;
    }

    public static ReturnValue vaccineSold(Integer vaccineID, Integer amount) {
        return OK;
    }

    public static ReturnValue vaccineProduced(Integer vaccineID, Integer amount) {
        return OK;
    }

    public static Boolean isLabPopular(Integer labID) {
        return true;
    }

    public static Integer getIncomeFromVaccine(Integer vaccineID) {
        return 0;
    }

    public static Integer getTotalNumberOfWorkingVaccines() {
        return 0;
    }

    public static Integer getTotalWages(Integer labID) {
        return 0;
    }

    public static Integer getBestLab() {
        return 0;
    }

    public static String getMostPopularCity() {
        return "";
    }

    public static ArrayList<Integer> getPopularLabs() {
        return new ArrayList<>();
    }

    public static ArrayList<Integer> getMostRatedVaccines() {
        return new ArrayList<>();
    }

    public static ArrayList<Integer> getCloseEmployees(Integer employeeID) {
        return new ArrayList<>();
    }

    private static PreparedStatement setAddLabParams(Connection connection, String stmt, Lab lab)
            throws SQLException {
        PreparedStatement pstmt = null;
        try {
            pstmt = connection.prepareStatement(stmt);
            pstmt.setInt(1, lab.getId());
            pstmt.setString(2, lab.getName());
            pstmt.setString(3, lab.getCity());
            pstmt.setBoolean(4, lab.getIsActive());
        } catch (Exception e) {
            throw e;
        }

        return pstmt;
    }

    private static PreparedStatement setAddEmployeeParams(Connection connection, String stmt, Employee employee)
            throws SQLException {
        PreparedStatement pstmt = null;
        try {
            pstmt = connection.prepareStatement(stmt);
            pstmt.setInt(1, employee.getId());
            pstmt.setString(2, employee.getName());
            pstmt.setString(3, employee.getCity());
        } catch (Exception e) {
            throw e;
        }

        return pstmt;
    }

    private static PreparedStatement setAddVaccineParams(Connection connection, String stmt, Vaccine vaccine)
            throws SQLException {
        PreparedStatement pstmt = null;
        try {
            //TODO might be updated
            pstmt = connection.prepareStatement(stmt);
            pstmt.setInt(1, vaccine.getId());
            pstmt.setString(2, vaccine.getName());
            pstmt.setInt(3, vaccine.getCost());
            pstmt.setInt(4, vaccine.getUnits());
            pstmt.setInt(5, vaccine.getProductivity());

        } catch (Exception e) {
            throw e;
        }

        return pstmt;
    }

    private static ReturnValue getError(SQLException e) {
        switch (getErrorCode(e)) {
            case CHECK_VIOLATION: return BAD_PARAMS; // TODO make sure
            case NOT_NULL_VIOLATION: return BAD_PARAMS;
            case FOREIGN_KEY_VIOLATION: return NOT_EXISTS;
            case UNIQUE_VIOLATION: return ALREADY_EXISTS;
            default: return ERROR;
        }
    }

    private static PostgreSQLErrorCodes getErrorCode(SQLException e) {
        int code = Integer.parseInt(e.getSQLState());
        switch (code) {
            case 23502: return NOT_NULL_VIOLATION;
            case 23503: return FOREIGN_KEY_VIOLATION;
            case 23505: return UNIQUE_VIOLATION;
            default: return CHECK_VIOLATION;
        }
    }

    private static void handleException(SQLException e) {
        // TODO make sure
        e.printStackTrace();
    }

    /**
     * Closes a prepared statement after it's execution.
     */
    private static void closeQuery(PreparedStatement query) {
        try {
            if (null != query) {
                query.close();
            }
        } catch (SQLException e) {
            handleException(e);
        }
    }

    private static void closeResultSet(ResultSet res) {
        try {
            if (null != res) {
                res.close();
            }
        } catch (SQLException e) {
            handleException(e);
        }
    }

    private static void closeConnection(Connection connection) {
        try {
            if (null != connection) {
                connection.close();
            }
        } catch (SQLException e) {
            handleException(e);
        }
    }

    private static void closeAll(Connection connection, PreparedStatement pstmt, ResultSet res) {
        closeConnection(connection);
        closeQuery(pstmt);
        closeResultSet(res);
    }

    /**
     * In case a ResultSet was not received.
     */
    private static void closeAll(Connection connection, PreparedStatement pstmt) {
        closeConnection(connection);
        closeQuery(pstmt);
    }

    /**
     * Receives a string query & a connection and creates a query, without closing the connection afterwards.
     */
    private static void createQuery(Connection connection, String stmt) {
        PreparedStatement pstmt = null;

        try {
            pstmt = connection.prepareStatement(stmt);
            pstmt.execute();
        } catch (SQLException e) {
            handleException(e);
        } finally {
            closeQuery(pstmt);
        }
    }

    /**
     * Receives a list of queries & a connection and creates queries, without closing the connection afterwards.
     */
    private static void createQueries(Connection connection, List<String> stmts) {
        PreparedStatement pstmt = null;

        try {
            for (String stmt: stmts) {
                pstmt = connection.prepareStatement(stmt);
                pstmt.execute();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            closeQuery(pstmt);
        }
    }

    /**
     * Receives a string query, creates a connection and a query, and closes the connection.
     */
    private static void createQueryAndConnection(String stmt) {
        Connection connection = DBConnector.getConnection();
        if (null != connection) {
            createQuery(connection, stmt);
            closeConnection(connection);
        }
    }

    /**
     * Receives a list of queries, creates a connection and creates queries, and closes the connection.
     */
    private static void createQueriesAndConnection(List<String> stmts) {
        Connection connection = DBConnector.getConnection();
        if (null != connection) {
            createQueries(connection, stmts);
            closeConnection(connection);
        }
    }

    /**
     * Opens a connection and drops a single table
     */
    private static void dropTable(String table) {
        // TODO add drop to views
        // TODO CASCADE?

        String stmt = String.format("DROP TABLE IF EXISTS %s", table);
        createQueryAndConnection(stmt);
    }

    private static ReturnValue removeEntryFromTable(String table, int id) {
        Connection connection = DBConnector.getConnection();
        PreparedStatement pstmt = null;
        ReturnValue retVal = null;
        if (null == connection) {
            return ERROR;
        }

        try {
            pstmt = connection.prepareStatement(String.format("DELETE FROM %s WHERE id = ?", table));
            pstmt.setInt(1, id);
            int deleted = pstmt.executeUpdate();
            if (0 == deleted) {
                retVal = NOT_EXISTS;
            } else {
                retVal = OK;
            }

        } catch (SQLException e) {
            handleException(e);
            retVal = ERROR;
        } finally {
            closeAll(connection, pstmt);
        }

        return retVal;
    }
}