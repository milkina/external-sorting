package sort.dao;

import sort.model.LineEntry;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.util.List;

public class LineEntryDAO {
    private static final String CREATE_TABLE_QUERY =
            "CREATE TABLE %s "
                    + "(id INT(5) NOT NULL AUTO_INCREMENT,"
                    + " %s "
                    + "PRIMARY KEY(id))";
    private static final String INSERT_QUERY =
            "INSERT INTO users (username) VALUES ('sidorov')";
    private Connection connection;

    public LineEntryDAO() {
        try {
            connection = ConnectorDB.getConnection();
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }

    public void close() {
        try {
            connection.close();
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }

    public String createTable(LineEntry lineEntry) {
        String tableName = generateTableName();
        try (Statement statement = connection.createStatement()) {
            String query = createQueryString(lineEntry, tableName);
            statement.executeUpdate(query);
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
        return tableName;
    }

    private String generateTableName() {
        String tableName = "sorted_entries_" + LocalDateTime.now().toString();
        tableName = tableName.replaceAll("[-:.]", "");
        return tableName;
    }

    private String createQueryString(LineEntry lineEntry, String tableName) {
        StringBuilder stringBuilder = new StringBuilder();
        for (int i = 0; i < lineEntry.getColumns().length; i++) {
            String columnStr = String.format(" column%s VARCHAR(50), ", i);
            stringBuilder.append(columnStr);
        }
        return String.format(CREATE_TABLE_QUERY, tableName, stringBuilder.toString());
    }

    public void createEntities(List<LineEntry> list, String tableName) {
        try (Statement statement = connection.createStatement()) {
            for (LineEntry lineEntry : list) {
                String query = getCreateQuery(tableName, lineEntry);
                statement.addBatch(query);
            }
            statement.executeBatch();
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }

    private String getCreateQuery(String tableName, LineEntry lineEntry) {
        StringBuilder columns = new StringBuilder();
        StringBuilder values = new StringBuilder();
        for (int i = 0; i < lineEntry.getColumns().length; i++) {
            columns.append(String.format("column%d,", i));
            values.append(String.format("'%s',", lineEntry.getColumns()[i]));
        }
        int columnsLength = columns.length();
        int valuesLength = values.length();
        columns.delete(columnsLength - 1, columnsLength);
        values.delete(valuesLength - 1, valuesLength);
        return String.format("INSERT INTO %s (%s) VALUES (%s)",
                tableName, columns.toString(), values.toString());
    }
}
