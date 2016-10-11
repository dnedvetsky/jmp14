package dbc;

import java.sql.*;
import java.util.Arrays;
import java.util.List;

/**
 * Created by Dmitry on 12.09.2016.
 */
public class DataBaseDriver {
    private final String SQLITECONNECTION = "jdbc:sqlite:";

    public DataBaseDriver() {
        try {
            Class.forName("org.sqlite.JDBC");
        } catch (ClassNotFoundException e) {
            System.out.println(e.toString());
        }
    }

    /**
     * Executes provided statement
     *
     * @param prepareStatement
     * @param vars
     */
    public void executeStatement(String prepareStatement, String... vars) {
        List<String> variables = Arrays.asList(vars);
        PreparedStatement preparedStatement = null;
        try (Connection connection = DriverManager.getConnection("jdbc:sqlite:database.db")) {
            preparedStatement = connection.prepareStatement(prepareStatement);
            for (int i = 1; i <= variables.size(); i++) {
                preparedStatement.setString(i, variables.get(i - 1));
            }
            preparedStatement.execute();
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            if (preparedStatement != null) {
                try {
                    preparedStatement.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public void printSpecifiedFieldFromQuery(String query, String... columns) {
        ResultSet rs;
        List<String> fieldslist = Arrays.asList(columns);
        try (Connection connection = DriverManager.getConnection("jdbc:sqlite:database.db")) {
            Statement statement = connection.createStatement();
            rs = statement.executeQuery(query);
            while (rs.next()) {
                for (String column : fieldslist) {
                    System.out.println(rs.getString(column));
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void executeBatchStatement(String prepareStatement, List<List<String>> lists) {
        PreparedStatement preparedStatement = null;
        try (Connection connection = DriverManager.getConnection("jdbc:sqlite:database.db")) {
            preparedStatement = connection.prepareStatement(prepareStatement);
            for (List<String> list : lists) {
                for (int i = 1; i <= list.size(); i++) {
                    preparedStatement.setString(i, list.get(i - 1));
                }
                preparedStatement.addBatch();
            }
            System.out.println("Attempting to add batch");
            preparedStatement.executeBatch();
            System.out.println("Added large batch");
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            if (preparedStatement != null) {
                try {
                    preparedStatement.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    /**
     * Executes provided statement
     *
     * @param prepareStatement
     */
    public void tableActions(String prepareStatement) {
        Statement statement = null;
        try (Connection connection = DriverManager.getConnection("jdbc:sqlite:database.db")) {
            statement = connection.createStatement();
            statement.execute(prepareStatement);
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            if (statement != null) {
                try {
                    statement.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public Integer getLatestID(String tableName) {
        ResultSet rs = null;
        try (Connection connection = DriverManager.getConnection("jdbc:sqlite:database.db")) {
            Statement statement = connection.createStatement();
            rs = statement.executeQuery("SELECT * FROM " + tableName + ";");
            int rowCount = 0;
            while (rs.next()) {
                rowCount++;
            }
            return rowCount;
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            try {
                if (rs != null) {
                    rs.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return null;
    }
}

