package corona.data;

import java.sql.*;

public class main {
    Connection connection = DBConnector.getConnection();
    PreparedStatement preparedStatement;
    {
        try {
            preparedStatement = connection.prepareStatement("SELECT * FROM *");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    Boolean ret;
    {
        try {
            ret = preparedStatement.execute();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

}
