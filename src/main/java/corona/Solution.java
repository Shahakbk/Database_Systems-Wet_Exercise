package corona;


import corona.business.Employee;
import corona.business.Lab;
import corona.business.ReturnValue;
import corona.business.Vaccine;
import corona.data.DBConnector;

import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static corona.business.ReturnValue.OK;

public class Solution {
    private static final String CREATE_TABLE_LABS =
            "CREATE TABLE Labs\n" +
            "(\n" +
            "    id integer NOT NULL,\n" +
            "    name text NOT NULL,\n" +
            "    city text NOT NULL,\n" +
            "    active boolean NOT NULL,\n" +
            "    PRIMARY KEY (id),\n" +
            "    CHECK (id > 0)\n" +
            ")";
    private static final String CREATE_TABLE_EMPLOYEES =
            "CREATE TABLE Employees\n" +
                    "(\n" +
                    "    id integer NOT NULL,\n" +
                    "    name text NOT NULL,\n" +
                    "    city_of_birth text NOT NULL,\n" +
                    "    PRIMARY KEY (id),\n" +
                    "    CHECK (id > 0)\n" +
                    ")";
    private static final String CREATE_TABLE_VACCINES =
            "CREATE TABLE Vaccines\n" +
                    "(\n" +
                    "    id integer NOT NULL,\n" +
                    "    name text NOT NULL,\n" +
                    "    cost integer NOT NULL,\n" +
                    "    units_in_stock integer NOT NULL,\n" +
                    "    productivity integer NOT NULL,\n" +
                    "    PRIMARY KEY (id),\n" +
                    "    CHECK (id > 0),\n" +
                    "    CHECK (cost >= 0),\n" +
                    "    CHECK (units_in_stock >= 0),\n" +
                    "    CHECK (productivity >= 0)\n" +
                    ")";

    /**
     * Closes a prepared statement after it's execution.
     */
    private static void closeQuery(PreparedStatement query) {
        try {
            if (null != query) {
                query.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
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
            e.printStackTrace();
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

            try {
                connection.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * Receives a list of queries, creates a connection and creates queries, and closes the connection.
     */
    private static void createQueriesAndConnection(List<String> stmts) {
        Connection connection = DBConnector.getConnection();
        if (null != connection) {
            createQueries(connection, stmts);

            try {
                connection.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

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
     * Opens a connection and drops a single table
     */
    private static void dropTable(String table) {
        // TODO add drop to views
        // TODO CASCADE?

        String stmt = String.format("DROP TABLE IF EXISTS %s", table);
        createQueryAndConnection(stmt);
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

        return OK;
    }

    public static Lab getLabProfile(Integer labID) {
        return new Lab();
    }

    public static ReturnValue deleteLab(Lab lab) {
        return OK;
    }

    public static ReturnValue addEmployee(Employee employee) {
        return OK;
    }

    public static Employee getEmployeeProfile(Integer employeeID) {
        return new Employee();
    }

    public static ReturnValue deleteEmployee(Employee employee) {
        return OK;
    }

    public static ReturnValue addVaccine(Vaccine vaccine) {
        return OK;
    }

    public static Vaccine getVaccineProfile(Integer vaccineID) {
        return new Vaccine();
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
}

