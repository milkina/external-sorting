package sort.controller;

import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import sort.io.CSVWriter;
import sort.model.FileItem;
import sort.model.LineEntry;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ExternalSortingTest {
    private static final String CATALOG_NAME = "csv\\tmp\\";
    private static final int ROW_NUMBER = 20;
    private List<FileItem> fileItems = new ArrayList<>();
    private String[] fileNames = {"temp1.csv", "temp2.csv", "temp3.csv"};

    @BeforeMethod
    public void initialize() {
        for (String fileName : fileNames) {
            fileItems.add(new FileItem(ROW_NUMBER, CATALOG_NAME + fileName));
            try (CSVWriter csvWriter = new CSVWriter(CATALOG_NAME + fileName)) {
                LineEntry lineEntry = new LineEntry(new String[]{fileName});
                csvWriter.write(lineEntry);
            } catch (IOException e) {
                e.printStackTrace();
            }
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
    public void increaseCurrentRow() {
        ExternalSorting externalSorting = new ExternalSorting("test.csv", 1, 200);
        ExternalSorting.setFileItems(fileItems);
        FileItem minFileItem = fileItems.get(0);
        minFileItem.setCurrentRow(ROW_NUMBER - 1);
        File file = new File(minFileItem.getName());

        Assert.assertTrue(fileItems.contains(minFileItem));
        Assert.assertTrue(file.exists());

        externalSorting.increaseCurrentRow(minFileItem);

        Assert.assertEquals(minFileItem.getCurrentRow(), ROW_NUMBER);
        Assert.assertTrue(!fileItems.contains(minFileItem));
        Assert.assertTrue(!file.exists());
    }

    @Test
    public void findMinLineEntryTest() {
        ExternalSorting externalSorting = new ExternalSorting("temp.csv", 1, 30);
        ExternalSorting.setFileItems(fileItems);
        LineEntry lineEntry = externalSorting.findMinLineEntry();
        Assert.assertNotNull(lineEntry);
        Assert.assertEquals(lineEntry.getColumns()[0], fileNames[0]);
    }
}
