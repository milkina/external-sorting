package sort.dao;

import sort.model.LineEntry;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.LinkedList;
import java.util.List;

public class LineEntryDAO {
    private static final String CREATE_TABLE_QUERY =
            "CREATE TABLE %s "
                    + "(id INT(5) NOT NULL AUTO_INCREMENT,"
                    + " %s"
                    + "PRIMARY KEY(id))";
    public static final String INSERT_DATA_QUERY = "INSERT INTO %s (%s) VALUES (%s)";
    public static final String COLUMN_ITEM = "column%d,";
    public static final String VALUE_ITEM = "'%s',";
    public static final String SELECT_COLUMNS_COUNT = "SELECT * FROM %s";
    public static final String SELECT_ROWS_COUNT = "SELECT COUNT(*) FROM %s";
    private Connection connection;

    /**
     * Open connection to DB
     */
    public void openConnection() {
        try {
            connection = ConnectorDB.getConnection();
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }

    /**
     * Close DB connection
     */
    public void close() {
        try {
            connection.close();
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }

    /**
     * Create table in DB. CSV's can have different number of columns
     *
     * @param columnNumber number of columns in the table
     * @return table name. If table doesn't created, the returned result will be null
     */
    public String createTable(int columnNumber) {
        String tableName;
        try (Statement statement = connection.createStatement()) {
            tableName = generateTableName();
            String query = createTableQueryString(columnNumber, tableName);
            statement.executeUpdate(query);
        } catch (SQLException e) {
            System.out.println(e.getMessage());
            tableName = null;
        }
        return tableName;
    }

    /**
     * Generate table name with current date and time
     *
     * @return table name
     */
    private String generateTableName() {
        DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy_MM_dd_hh_mm_ss_SSSS");
        return "sorted_entries_" + LocalDateTime.now().format(dateTimeFormatter);
    }

    /**
     * Create SQL string for creating table
     *
     * @param columnNumber - number of columns in the table
     * @param tableName
     * @return SQL string
     */
    public String createTableQueryString(int columnNumber, String tableName) {
        StringBuilder stringBuilder = new StringBuilder();
        for (int i = 0; i < columnNumber; i++) {
            String columnStr = String.format("column%s VARCHAR(50), ", i);
            stringBuilder.append(columnStr);
        }
        return String.format(CREATE_TABLE_QUERY, tableName, stringBuilder.toString());
    }

    /**
     * Insert rows in the DB
     *
     * @param list
     * @param tableName
     */
    public void createEntities(List<LineEntry> list, String tableName) {
        try (Statement statement = connection.createStatement()) {
            for (LineEntry lineEntry : list) {
                String query = createInsertQuery(tableName, lineEntry);
                statement.addBatch(query);
            }
            statement.executeBatch();
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }

    /**
     * Create SQL string for insert query
     *
     * @param tableName
     * @param lineEntry
     * @return SQL string
     */
    public String createInsertQuery(String tableName, LineEntry lineEntry) {
        StringBuilder columns = new StringBuilder();
        StringBuilder values = new StringBuilder();
        for (int i = 0; i < lineEntry.getColumns().length; i++) {
            columns.append(String.format(COLUMN_ITEM, i));
            values.append(String.format(VALUE_ITEM, lineEntry.getColumns()[i]));
        }
        int columnsLength = columns.length();
        int valuesLength = values.length();
        columns.delete(columnsLength - 1, columnsLength);
        values.delete(valuesLength - 1, valuesLength);
        return String.format(INSERT_DATA_QUERY,
                tableName, columns.toString(), values.toString());
    }

    /**
     * Get number of rows in the table tableName
     *
     * @param tableName
     * @return number of rows
     */
    public int getRowsCount(String tableName) {
        int result = 0;
        try (Statement statement = connection.createStatement()) {
            String selectString = String.format(SELECT_ROWS_COUNT, tableName);
            ResultSet resultSet = statement.executeQuery(selectString);
            if (resultSet.next()) {
                result = resultSet.getInt(1);
            }
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
        return result;
    }

    /**
     * Return number of columns in the table tableName
     *
     * @param tableName
     * @return
     */
    public int getColumnsCount(String tableName) {
        int result = 0;
        try (Statement statement = connection.createStatement()) {
            String selectString = String.format(SELECT_COLUMNS_COUNT, tableName);
            ResultSet resultSet = statement.executeQuery(selectString);
            if (resultSet.next()) {
                result = resultSet.getMetaData().getColumnCount();
            }
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
        return result;
    }

    public List<LineEntry> findEntities(int from, int maxSize, String tableName) {
        List<LineEntry> result = new LinkedList<>();
        try (Statement statement = connection.createStatement()) {
            String queryString = String.format("SELECT * FROM %s WHERE id>%d ORDER BY id LIMIT %d",
                    tableName, from, maxSize);
            ResultSet resultSet = statement.executeQuery(queryString);
            while (resultSet.next()) {
                int columnsCount = resultSet.getMetaData().getColumnCount() - 1;
                String[] columns = new String[columnsCount];
                for (int i = 0; i < columnsCount; i++) {
                    columns[i] = resultSet.getString(i + 2);
                }
                LineEntry lineEntry = new LineEntry(columns);
                result.add(lineEntry);
            }
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
        return result;
    }
}
