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
            "    revenue integer NOT NULL,\n" +
            "    PRIMARY KEY (id),\n" +
            "    CHECK (id > 0),\n" +
            "    CHECK (cost >= 0),\n" +
            "    CHECK (unitsInStock >= 0),\n" +
            "    CHECK (productivity >= 0),\n" +
            "    CHECK (revenue >= 0)\n" +
            ")";
    private static final String CREATE_TABLE_EMPLOYED_AT =
            "CREATE TABLE EmployedAt\n" +
            "(\n" +
            "   employeeId integer NOT NULL,\n" +
            "   labId integer NOT NULL,\n" +
            "   wage integer NOT NULL,\n" +
            "   FOREIGN KEY (employeeId) REFERENCES Employees(id) ON DELETE CASCADE,\n" +
            "   FOREIGN KEY (labId) REFERENCES Labs(id) ON DELETE CASCADE,\n" +
            "   PRIMARY KEY (labId, employeeId),\n" +
            "   CHECK (wage >= 0)" +
            ")";
    private static final String CREATE_TABLE_PRODUCED_BY =
            "CREATE TABLE ProducedBy\n" +
            "(\n" +
            "   labId integer NOT NULL,\n" +
            "   vaccineId integer NOT NULL,\n" +
            "   FOREIGN KEY (labId) REFERENCES Labs(id) ON DELETE CASCADE,\n" +
            "   FOREIGN KEY (vaccineId) REFERENCES Vaccines(id) ON DELETE CASCADE,\n" +
            "   PRIMARY KEY (labId, vaccineId)\n" +
            ")";
    private static final String CREATE_VIEW_POPULAR_LABS =
            "CREATE VIEW PopularLabs AS\n" +
            "   SELECT Labs.id as id\n" +
            "   FROM Labs\n" +
            "   WHERE id NOT IN\n" +
            "       (\n" +
            "           SELECT L.id\n" +
            "           FROM Vaccines AS V, ProducedBy as P, Labs as L\n" +
            "           WHERE V.id = P.vaccineId AND\n" +
            "               P.labId = L.id AND \n" +
            "               V.productivity <= 20\n" +
            "       )";
    private static final String UPDATE_TABLE_VACCINE_SOLD =
            "UPDATE Vaccines\n" +
            "   SET\n" +
            "       cost = cost * 2,\n" +
            "       productivity = LEAST (100, productivity + 15),\n" +
            "       unitsInStock = unitsInStock - ?,\n" +
            "       revenue = revenue + cost * ?\n" +
            "   WHERE id = ?";
    private static final String UPDATE_TABLE_VACCINE_PRODUCED =
            "UPDATE Vaccines\n" +
            "   SET\n" +
            "       cost = cost / 2,\n" +
            "       productivity = GREATEST (0, productivity - 15),\n" +
            "       unitsInStock = unitsInStock + ?\n" +
            "   WHERE id = ?";
    private static final String GET_TOTAL_WAGES =
            "SELECT SUM (wage) AS totalWages\n" +
            "FROM EmployedAt AS E\n" +
            "WHERE\n" +
            "   E.labId =\n" +
            "       (\n" +
            "           SELECT id FROM Labs\n" +
            "           WHERE id = ? AND isActive = true\n" +
            "       )\n" +
            "   HAVING\n" +
            "       COUNT (E.employeeId) > 1";
    private static final String GET_BEST_LAB =
            "SELECT L.id AS bestID\n" +
            "FROM EmployedAt AS EA, Labs as L, Employees as E\n" +
            "WHERE\n" +
            "   L.city = E.cityOfBirth AND\n" +
            "   L.id = EA.labId AND\n" +
            "   E.id = EA.employeeId\n" +
            "GROUP BY\n" +
            "   L.id\n" +
            "ORDER BY\n" +
            "   COUNT(L.id) DESC,\n" +
            "   L.id ASC\n" +
            "LIMIT 1";
    private static final String GET_MOST_POPULAR_CITY =
            "SELECT E.cityOfBirth AS mostPopularCity\n" +
            "FROM EmployedAt AS EA, Employees as E\n" +
            "WHERE\n" +
            "   E.id = EA.employeeId\n" +
            "GROUP BY\n" +
            "   E.cityOfBirth\n" +
            "ORDER BY\n" +
            "   COUNT(E.cityOfBirth) DESC,\n" +
            "   E.cityOfBirth DESC\n" +
            "LIMIT 1";
    private static final String GET_POPULAR_LABS =
            "SELECT id as mostPopularLabs\n" +
            "FROM PopularLabs\n" +
            "WHERE\n" +
            "   id IN \n" +
            "   (\n" +
            "       SELECT labId\n" +
            "       FROM ProducedBy\n" +
            "   )\n" +
            "ORDER BY\n" +
            "   id ASC\n" +
            "LIMIT 3";
/*    private static final String GET_POPULAR_LABS =
            "SELECT labId FROM PopularLabs\n" +
            "WHERE labId IN" +
            "       SELECT labId\n" +
            "       FROM ProducedBy)\n" +
            "ORDER BY\n" +
            "   labId ASC\n" +
            "LIMIT 3";*/
    private static final String GET_MOST_RATED_VACCINES =
            "SELECT id AS mostRatedVaccines\n" +
            "FROM Vaccines\n" +
            "ORDER BY\n" +
            "   (productivity + unitsInStock - cost) DESC,\n" +
            "   id ASC,\n" +
            "LIMIT 10";
    private static final String GET_CLOSE_EMPLOYEES =
            "SELECT E.id AS closeEmployees\n" +
            "FROM Employees AS E\n" +
            "WHERE\n" +
            "   ? != E.id AND \n" +
            "   ? IN\n" +
            "       (SELECT id\n" +
            "       FROM Employees)\n" +
            "   AND\n" +
            "       (SELECT COUNT(EQ.labId) FROM EmployedAt AS EQ WHERE EQ.employeeId = ?)\n" +
            "       <= 2 * (SELECT COUNT(NEQ.labId) FROM EmployedAt AS NEQ WHERE NEQ.employeeId = E.id AND\n" +
            "           NEQ.labId IN (SELECT labId FROM EmployedAt AS W2 WHERE W2.employeeId = ?))\n" +
            "ORDER BY\n" +
            "   E.id ASC\n" +
            "LIMIT 10";

    public static void createTables() {
        List<String> queries = Arrays.asList(
                CREATE_TABLE_LABS,
                CREATE_TABLE_EMPLOYEES,
                CREATE_TABLE_VACCINES,
                CREATE_TABLE_EMPLOYED_AT,
                CREATE_TABLE_PRODUCED_BY,
                CREATE_VIEW_POPULAR_LABS
        );
        createQueriesAndConnection(queries);
    }

    /**
     * Clears all of the tables in the DB.
     */
    public static void clearTables() {
        List<String> tables = Arrays.asList(
                "Labs",
                "Employees",
                "Vaccines",
                "EmployedAt",
                "ProducedBy",
                "PopularLabs"
        );
        tables = tables.stream().map(s -> String.format("DELETE FROM %s", s)).collect(Collectors.toList());
        createQueriesAndConnection(tables);
    }

    /**
     * Creates the queries to drop the tables, opens a connection and drops them.
     */
    public static void dropTables() {
        List<String> tables = Arrays.asList(
                "Labs",
                "Employees",
                "Vaccines",
                "EmployedAt",
                "ProducedBy",
                "PopularLabs"
        );
        tables = tables.stream().map(s -> String.format("DROP TABLE IF EXISTS %s CASCADE", s)).collect(Collectors.toList());
        createQueriesAndConnection(tables);
        createQueryAndConnection("DROP VIEW IF EXISTS PopularLabs");
    }

    public static ReturnValue addLab(Lab lab) {
        if (null == lab) {
            return BAD_PARAMS;
        }

        ReturnValue retVal = OK;
        String query =
                "INSERT INTO Labs \n" +
                "VALUES (?, ?, ?, ?)";
        PreparedStatement pstmt = null;
        Connection connection = DBConnector.getConnection();

        if (null != connection) {
            try {
                pstmt = setLabParams(connection, query, lab);
                pstmt.execute();
            } catch (SQLException e) {
                handleException(e);
                retVal = getError(e);
            } finally {
                closeAll(connection, pstmt);
            }
        }

        return retVal;
    }

    public static Lab getLabProfile(Integer labID) {
        Connection connection = DBConnector.getConnection();
        ResultSet resultSet = null;
        if (null == connection || null == labID) return Lab.badLab();

        PreparedStatement pstmt = null;
        Lab lab = new Lab();
        try {
            pstmt = connection.prepareStatement("SELECT * FROM Labs WHERE id = ?");
            pstmt.setInt(1, labID);
            resultSet = pstmt.executeQuery();
            if (!resultSet.next()) {
                return Lab.badLab();
            }
            lab.setId(labID);
            lab.setName(resultSet.getString("name"));
            lab.setCity(resultSet.getString("city"));
            lab.setIsActive(resultSet.getBoolean("isActive"));
            return lab;

        } catch (SQLException e) {
            handleException(e);
            lab = Lab.badLab();
        } finally {
            closeAll(connection, pstmt, resultSet);
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
        String query =
                "INSERT INTO Employees \n" +
                "VALUES (?, ?, ?)";
        PreparedStatement pstmt = null;
        Connection connection = DBConnector.getConnection();

        if (null != connection) {
            try {
                pstmt = setEmployeeParams(connection, query, employee);
                pstmt.execute();
            } catch (SQLException e) {
                handleException(e);
                retVal = getError(e);
            } finally {
                closeAll(connection, pstmt);
            }
        }

        return retVal;
    }

    public static Employee getEmployeeProfile(Integer employeeID) {
        Connection connection = DBConnector.getConnection();
        ResultSet resultSet = null;
        if (null == connection || null == employeeID) return Employee.badEmployee();

        PreparedStatement pstmt = null;
        Employee employee = new Employee();
        try {
            pstmt = connection.prepareStatement("SELECT * FROM Employees WHERE id = ?");
            pstmt.setInt(1, employeeID);
            resultSet = pstmt.executeQuery();
            if (!resultSet.next()) {
                return Employee.badEmployee();
            }
            employee.setId(employeeID);
            employee.setName(resultSet.getString("name"));
            employee.setCity(resultSet.getString("cityOfBirth"));
            return employee;

        } catch (SQLException e) {
            handleException(e);
            employee = Employee.badEmployee();
        } finally {
            closeAll(connection, pstmt, resultSet);
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
        String query =
                "INSERT INTO Vaccines \n" +
                "VALUES (?, ?, ?, ?, ?, 0)";
        PreparedStatement pstmt = null;
        Connection connection = DBConnector.getConnection();

        if (null != connection) {
            try {
                pstmt = setVaccineParams(connection, query, vaccine);
                pstmt.execute();
            } catch (SQLException e) {
                handleException(e);
                retVal = getError(e);
            } finally {
                closeAll(connection, pstmt);
            }
        }

        return retVal;
    }

    public static Vaccine getVaccineProfile(Integer vaccineID) {
        Connection connection = DBConnector.getConnection();
        ResultSet resultSet = null;
        if (null == connection || null == vaccineID) return Vaccine.badVaccine();

        PreparedStatement pstmt = null;
        Vaccine vaccine = new Vaccine();
        try {
            pstmt = connection.prepareStatement("SELECT * FROM Vaccines WHERE id = ?");
            pstmt.setInt(1, vaccineID);
            resultSet = pstmt.executeQuery();
            if (!resultSet.next()) {
                return Vaccine.badVaccine();
            }
            vaccine.setId(vaccineID);
            vaccine.setName(resultSet.getString("name"));
            vaccine.setCost(resultSet.getInt("cost"));
            vaccine.setUnits(resultSet.getInt("unitsInStock"));
            vaccine.setProductivity(resultSet.getInt("productivity"));
            return vaccine;

        } catch (SQLException e) {
            handleException(e);
            vaccine = Vaccine.badVaccine();
        } finally {
            closeAll(connection, pstmt, resultSet);
        }

        return vaccine;
    }

    public static ReturnValue deleteVaccine(Vaccine vaccine) {
        return removeEntryFromTable("Vaccines", vaccine.getId());
    }

    public static ReturnValue employeeJoinLab(Integer employeeID, Integer labID, Integer salary) {
        if (null == labID || null == employeeID || null == salary)
            return BAD_PARAMS;
        //TODO deal with salary < 0

        ReturnValue retVal = OK;
        String query =
                "INSERT INTO EmployedAt \n" +
                "VALUES (?, ?, ?)";
        PreparedStatement pstmt = null;
        Connection connection = DBConnector.getConnection();

        if (null != connection) {
            try {
                pstmt = setEmployedAtParams(connection, query, labID, employeeID, salary);
                pstmt.execute();
            } catch (SQLException e) {
                handleException(e);
                retVal = getError(e);
            } finally {
                closeAll(connection, pstmt);
            }
        }

        return retVal;

    }

    public static ReturnValue employeeLeftLab(Integer labID, Integer employeeID) {
        return removeEntryFromJoinedTable("EmployedAt", "employeeId", "labId", employeeID, labID);
    }

    public static ReturnValue labProduceVaccine(Integer vaccineID, Integer labID) {
        if (null == vaccineID || null == labID)
            return BAD_PARAMS;

        ReturnValue retVal = OK;
        String query =
                "INSERT INTO ProducedBy \n" +
                "VALUES (?, ?)";
        PreparedStatement pstmt = null;
        Connection connection = DBConnector.getConnection();

        if (null != connection) {
            try {
                pstmt = setProducedByParams(connection, query, labID, vaccineID);
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

    public static ReturnValue labStoppedProducingVaccine(Integer labID, Integer vaccineID) {
        return removeEntryFromJoinedTable("ProducedBy", "labId", "vaccineId", labID, vaccineID);
    }

    public static ReturnValue vaccineSold(Integer vaccineID, Integer amount) {
        Connection connection = DBConnector.getConnection();
        ReturnValue retVal = null;
        if (null == vaccineID || null == amount) return BAD_PARAMS;
        if (null == connection) return ERROR;
        // TODO check amount >= 0

        PreparedStatement pstmt = null;
        try {
            pstmt = connection.prepareStatement(UPDATE_TABLE_VACCINE_SOLD);
            pstmt.setInt(1, amount);
            pstmt.setInt(2, amount);
            pstmt.setInt(3, vaccineID);

            int updated = pstmt.executeUpdate();
            if (0 == updated) {
                retVal = NOT_EXISTS;
            } else {
                retVal = OK;
            }

        } catch (SQLException e) {
            handleException(e);
            retVal = getError(e);
        } finally {
            closeAll(connection, pstmt);
        }

        return retVal;
    }

    public static ReturnValue vaccineProduced(Integer vaccineID, Integer amount) {
        Connection connection = DBConnector.getConnection();
        ReturnValue retVal = null;
        if (null == vaccineID || null == amount) return BAD_PARAMS;
        if (null == connection) return ERROR;

        PreparedStatement pstmt = null;
        try {
            pstmt = connection.prepareStatement("SELECT NULL WHERE ? >= 0");
            pstmt.setInt(1, amount);
            if (!pstmt.executeQuery().next()) return BAD_PARAMS;

            pstmt = connection.prepareStatement(UPDATE_TABLE_VACCINE_PRODUCED);
            pstmt.setInt(1, amount);
            pstmt.setInt(2, vaccineID);

            int updated = pstmt.executeUpdate();
            if (0 == updated) {
                retVal = NOT_EXISTS;
            } else {
                retVal = OK;
            }

        } catch (SQLException e) {
            handleException(e);
            retVal = getError(e);
        } finally {
            closeAll(connection, pstmt);
        }

        return retVal;
    }

    public static Boolean isLabPopular(Integer labID) {
        Connection connection = DBConnector.getConnection();
        ResultSet resultSet = null;
        Boolean res = true;

        if (null == labID || null == connection) return false;

        PreparedStatement pstmt = null;
        try {
            pstmt = connection.prepareStatement(
                    "SELECT id\n" +
                    "FROM PopularLabs\n" +
                    "WHERE id = ?"
            );
            pstmt.setInt(1, labID);

            resultSet = pstmt.executeQuery();
            if (!resultSet.next()) {
                res = false;
            }

        } catch (SQLException e) {
            handleException(e);
            res = false;
        } finally {
            closeAll(connection, pstmt, resultSet);
        }

        return res;
    }

    public static Integer getIncomeFromVaccine(Integer vaccineID) {
        Connection connection = DBConnector.getConnection();
        ResultSet resultSet = null;
        int res = 0;

        if (null == vaccineID || null == connection) return 0;

        PreparedStatement pstmt = null;
        try {
            pstmt = connection.prepareStatement(
                    "SELECT revenue\n" +
                        "FROM Vaccines\n" +
                        "WHERE id = ?"
            );
            pstmt.setInt(1, vaccineID);

            resultSet = pstmt.executeQuery();
            if (resultSet.next()) {
                res = resultSet.getInt("revenue");
            }

        } catch (SQLException e) {
            handleException(e);
            res = 0;
        } finally {
            closeAll(connection, pstmt, resultSet);
        }

        return res;
    }

    public static Integer getTotalNumberOfWorkingVaccines() {
        Connection connection = DBConnector.getConnection();
        ResultSet resultSet = null;
        int res = 0;

        if (null == connection) return 0;

        PreparedStatement pstmt = null;
        try {
            pstmt = connection.prepareStatement(
                    "SELECT SUM (unitsInStock) AS numberOfWorkingVaccines\n" +
                        "FROM Vaccines\n" +
                        "WHERE productivity > 20"
            );

            resultSet = pstmt.executeQuery();
            if (resultSet.next()) {
                res = resultSet.getInt("numberOfWorkingVaccines");
            }

        } catch (SQLException e) {
            handleException(e);
            res = 0;
        } finally {
            closeAll(connection, pstmt, resultSet);
        }

        return res;
    }

    public static Integer getTotalWages(Integer labID) {
        Connection connection = DBConnector.getConnection();
        ResultSet resultSet = null;
        int res = 0;

        if (null == connection || null == labID) return 0;

        PreparedStatement pstmt = null;
        try {
            pstmt = connection.prepareStatement(GET_TOTAL_WAGES);
            pstmt.setInt(1, labID);
            resultSet = pstmt.executeQuery();
            if (resultSet.next()) {
                res = resultSet.getInt("totalWages");
            }

        } catch (SQLException e) {
            handleException(e);
            res = 0;
        } finally {
            closeAll(connection, pstmt, resultSet);
        }

        return res;
    }

    public static Integer getBestLab() {
        Connection connection = DBConnector.getConnection();
        ResultSet resultSet = null;
        int res = 0;

        if (null == connection) return 0;

        PreparedStatement pstmt = null;
        try {
            pstmt = connection.prepareStatement(GET_BEST_LAB);
            resultSet = pstmt.executeQuery();
            if (resultSet.next()) {
                res = resultSet.getInt("bestID");
            }

        } catch (SQLException e) {
            handleException(e);
            res = 0;
        } finally {
            closeAll(connection, pstmt, resultSet);
        }

        return res;
    }

    public static String getMostPopularCity() {
        Connection connection = DBConnector.getConnection();
        ResultSet resultSet = null;
        String res = "";

        if (null == connection) return "";

        PreparedStatement pstmt = null;
        try {
            pstmt = connection.prepareStatement(GET_MOST_POPULAR_CITY);
            resultSet = pstmt.executeQuery();
            if (resultSet.next()) {
                res = resultSet.getString("mostPopularCity");
            }

        } catch (SQLException e) {
            handleException(e);
            res = "";
        } finally {
            closeAll(connection, pstmt, resultSet);
        }

        return res;
    }

    public static ArrayList<Integer> getPopularLabs() {
        Connection connection = DBConnector.getConnection();
        ResultSet resultSet = null;
        ArrayList<Integer> res = new ArrayList<Integer>();

        if (null == connection) return null;

        PreparedStatement pstmt = null;
        try {
            pstmt = connection.prepareStatement(GET_POPULAR_LABS);
            resultSet = pstmt.executeQuery();
            while (resultSet.next()) {
                res.add(resultSet.getInt("mostPopularLabs"));
            }

        } catch (SQLException e) {
            handleException(e);
        } finally {
            closeAll(connection, pstmt, resultSet);
        }

        return res;
    }

    public static ArrayList<Integer> getMostRatedVaccines() {
        Connection connection = DBConnector.getConnection();
        ResultSet resultSet = null;
        ArrayList<Integer> res = new ArrayList<Integer>();

        if (null == connection) return null;

        PreparedStatement pstmt = null;
        try {
            pstmt = connection.prepareStatement(GET_MOST_RATED_VACCINES);
            resultSet = pstmt.executeQuery();
            while (resultSet.next()) {
                res.add(resultSet.getInt("mostRatedVaccines"));
            }

        } catch (SQLException e) {
            handleException(e);
        } finally {
            closeAll(connection, pstmt, resultSet);
        }

        return res;
    }

    public static ArrayList<Integer> getCloseEmployees(Integer employeeID) {
        Connection connection = DBConnector.getConnection();
        ResultSet resultSet = null;
        ArrayList<Integer> res = new ArrayList<Integer>();

        if (null == connection) return null;

        PreparedStatement pstmt = null;
        try {
            pstmt = connection.prepareStatement(GET_CLOSE_EMPLOYEES);
            pstmt.setInt(1, employeeID);
            pstmt.setInt(2, employeeID);
            pstmt.setInt(3, employeeID);
            pstmt.setInt(4, employeeID);
            resultSet = pstmt.executeQuery();
            while (resultSet.next()) {
                res.add(resultSet.getInt("closeEmployees"));
            }

        } catch (SQLException e) {
            handleException(e);
        } finally {
            closeAll(connection, pstmt, resultSet);
        }

        return res;
    }

    private static PreparedStatement setLabParams(Connection connection, String stmt, Lab lab)
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

    private static PreparedStatement setEmployeeParams(Connection connection, String stmt, Employee employee)
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

    private static PreparedStatement setVaccineParams(Connection connection, String stmt, Vaccine vaccine)
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

    private static PreparedStatement setEmployedAtParams(Connection connection, String stmt, Integer labID,
                                                         Integer employeeID, Integer wage)
            throws SQLException {
        PreparedStatement pstmt = null;
        try {
            pstmt = connection.prepareStatement(stmt);
            pstmt.setInt(1, employeeID);
            pstmt.setInt(2, labID);
            pstmt.setInt(3, wage);

        } catch (Exception e) {
            throw e;
        }

        return pstmt;
    }

    private static PreparedStatement setProducedByParams(Connection connection, String stmt, Integer labID,
                                                         Integer vaccineID)
            throws SQLException {
        PreparedStatement pstmt = null;
        try {
            pstmt = connection.prepareStatement(stmt);
            pstmt.setInt(1, labID);
            pstmt.setInt(2, vaccineID);

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
        // TODO comment out
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
            handleException(e);
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

        String stmt = String.format("DROP TABLE IF EXISTS %s CASCADE", table);
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

    private static ReturnValue removeEntryFromJoinedTable(String table, String key1, String key2, int id1, int id2) {
        Connection connection = DBConnector.getConnection();
        PreparedStatement pstmt = null;
        ReturnValue retVal = null;
        if (null == connection) {
            return ERROR;
        }

        try {
            pstmt = connection.prepareStatement(String.format("DELETE FROM %s WHERE %s = ? AND %s = ?", table, key1, key2));
            pstmt.setInt(1, id1);
            pstmt.setInt(2, id2);
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