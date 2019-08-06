package sort.io;

import org.testng.Assert;
import org.testng.annotations.Test;
import sort.model.LineEntry;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class CSVWriterTest {
    private static final String CATALOG_NAME = "csv\\tmp\\";
    private static final String FILE_NAME = "temp.csv";

    private List<LineEntry> createList() {
        List<LineEntry> result = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            LineEntry lineEntry = new LineEntry(new String[]{String.valueOf(i)});
            result.add(lineEntry);
        }
        return result;
    }

    @Test
    public void writeTest() {
        List<LineEntry> list = createList();
        try (CSVWriter csvWriter = new CSVWriter(CATALOG_NAME + FILE_NAME);
             CSVReader csvReader = new CSVReader(CATALOG_NAME + FILE_NAME)) {
            csvWriter.writeForSingleThread(list);
            List<LineEntry> result = csvReader.readForSingleThread(list.size());
            Assert.assertEquals(list, result);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
