package sort.dao;

import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;
import sort.model.LineEntry;

import java.util.ArrayList;
import java.util.List;

public class LineEntryDAOTest {
    private LineEntryDAO lineEntryDAO = new LineEntryDAO();

    @DataProvider
    public static Object[][] insertQuery() {
        return new Object[][]
                {{"table1", new LineEntry(new String[]{"value0", "value1", "value2"}),
                        "INSERT INTO table1 (column0,column1,column2) VALUES ('value0','value1','value2')"}
                };
    }

    @DataProvider
    public static Object[][] createTableQuery() {
        return new Object[][]
                {{"table1", new LineEntry(new String[]{"value0", "value1", "value2"}),
                        "CREATE TABLE table1 "
                                + "(id INT(5) NOT NULL AUTO_INCREMENT,"
                                + " column0 VARCHAR(50),"
                                + " column1 VARCHAR(50),"
                                + " column2 VARCHAR(50),"
                                + " PRIMARY KEY(id))"}
                };
    }

    @Test(dataProvider = "insertQuery")
    public void createInsertQueryTest(String tableName, LineEntry lineEntry, String expectedResult) {
        String result = lineEntryDAO.createInsertQuery(tableName, lineEntry);
        Assert.assertEquals(expectedResult, result);
    }

    @Test(dataProvider = "createTableQuery")
    public void createTableQueryStringTest(String tableName, LineEntry lineEntry, String expectedResult) {
        String result = lineEntryDAO.createTableQueryString(lineEntry.getColumns().length, tableName);
        Assert.assertEquals(expectedResult, result);
    }

    @Test
    public void createTableTest() {
        LineEntry lineEntry = new LineEntry(new String[]{"value0", "value1"});
        lineEntryDAO.openConnection();
        String result = lineEntryDAO.createTable(lineEntry.getColumns().length);
        Assert.assertNotNull(result);
        lineEntryDAO.close();
    }

    @Test
    public void createEntitiesTest() {
        lineEntryDAO.openConnection();
        List<LineEntry> list = new ArrayList<>();
        String[] values = new String[]{"value0", "value1"};
        LineEntry lineEntry = new LineEntry(values);
        list.add(lineEntry);
        list.add(lineEntry);
        list.add(lineEntry);

        String tableName = lineEntryDAO.createTable(lineEntry.getColumns().length);
        lineEntryDAO.createEntities(list, tableName);

        int rowsCount = lineEntryDAO.getRowsCount(tableName);
        Assert.assertEquals(rowsCount, list.size());

        int columnsCount = lineEntryDAO.getColumnsCount(tableName);
        Assert.assertEquals(columnsCount, values.length + 1);

        lineEntryDAO.close();
    }
}
