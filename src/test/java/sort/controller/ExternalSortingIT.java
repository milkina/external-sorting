package sort.controller;

import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;
import sort.dao.LineEntryDAO;
import sort.model.LineEntry;
import sort.utility.GenerateCSVFile;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

public class ExternalSortingIT {
    private static final String TEST_CSV = "csv\\test.csv";
    private static final int ROWS_COUNT = 2000;
    private static final int COLUMNS_COUNT = 5;

    @BeforeClass
    public void generateFile() {
        GenerateCSVFile generateCSVFile = new GenerateCSVFile();
        generateCSVFile.generateFile(ROWS_COUNT, COLUMNS_COUNT, TEST_CSV);
    }

    @DataProvider
    public static Object[][] data() {
        return new Object[][]{{100},
                {200},
                {300},
                {400},
        };
    }

    @Test(dataProvider = "data")
    public void verify(int maxRows) {
        ExternalSorting externalSorting = new ExternalSorting(TEST_CSV, 2, maxRows);
        externalSorting.sort();
        String tableName = externalSorting.writeToDB();

        LineEntryDAO lineEntryDAO = new LineEntryDAO();
        lineEntryDAO.openConnection();
        int rowsCount = lineEntryDAO.getRowsCount(tableName);
        int columnsCount = lineEntryDAO.getColumnsCount(tableName);
        Assert.assertEquals(rowsCount, ROWS_COUNT);
        Assert.assertEquals(columnsCount, COLUMNS_COUNT + 1);

        int nextEntryIndex = 1;
        LineEntry lastEntry = null;
        while (nextEntryIndex < rowsCount) {
            List<LineEntry> list = lineEntryDAO.findEntities(nextEntryIndex, maxRows / 2 - 1, tableName);
            if (lastEntry != null) {
                list.set(0, lastEntry);
            }
            List<LineEntry> copiedList = new LinkedList<>(list);
            Collections.sort(list);
            Assert.assertEquals(list, copiedList);
            lastEntry = list.get(list.size() - 1);
            nextEntryIndex += maxRows / 2 - 1;
        }
        lineEntryDAO.close();
    }
}
