package sort.io;

import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import sort.model.FileItem;
import sort.model.LineEntry;

import java.io.File;
import java.io.IOException;
import java.util.List;

public class CSVReaderTest {
    private static final String CATALOG_NAME = "csv\\tmp\\";
    private static final String FILE_NAME = "temp.csv";

    @BeforeMethod
    public void initialize() {
        try (CSVWriter csvWriter = new CSVWriter(CATALOG_NAME + FILE_NAME)) {
            for (int i = 0; i < 10; i++) {
                LineEntry lineEntry = new LineEntry(new String[]{String.valueOf(i)});
                csvWriter.write(lineEntry);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @AfterMethod
    public void clear() {
        File catalog = new File(CATALOG_NAME);
        File[] files = catalog.listFiles();
        if (files != null) {
            for (File file : files) {
                file.delete();
            }
        }
    }

    @Test
    public void readTest() {
        try (CSVReader csvReader = new CSVReader(CATALOG_NAME + FILE_NAME)) {
            List<LineEntry> list = csvReader.readForSingleThread(5);
            Assert.assertNotNull(list);
            Assert.assertEquals(list.size(), 5);
            for (int i = 0; i < list.size(); i++) {
                LineEntry lineEntry = list.get(i);
                Assert.assertNotNull(lineEntry);
                Assert.assertNotNull(lineEntry.getColumns());
                Assert.assertNotNull(lineEntry.getColumns()[0]);
                Assert.assertEquals(lineEntry.getColumns()[0], String.valueOf(i));
            }
        }
    }

    @Test
    public void readLineTest() {
        int rowNumber = 7;
        try (CSVReader csvReader = new CSVReader(CATALOG_NAME + FILE_NAME)) {
            LineEntry lineEntry = csvReader.readLine(rowNumber);
            Assert.assertNotNull(lineEntry);
            Assert.assertNotNull(lineEntry.getColumns());
            Assert.assertNotNull(lineEntry.getColumns()[0]);
            Assert.assertEquals(lineEntry.getColumns()[0], String.valueOf(rowNumber - 1));
        }
    }
}
