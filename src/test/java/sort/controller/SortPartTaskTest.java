package sort.controller;

import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import sort.io.CSVReader;
import sort.model.FileItem;
import sort.model.LineEntry;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import static sort.controller.SortPartTask.OUTPUT_FILE_NAME;

public class SortPartTaskTest {
    @BeforeClass
    public void initialize() {
        LineEntry.setKeyIndex(1);
        ExternalSorting.setFileItems(new ArrayList<>());
    }

    @Test
    public void generateTmpFileItemTest() {
        SortPartTask sortPartTask = new SortPartTask(new ArrayList<>());
        FileItem fileItem = sortPartTask.generateTmpFileItem(300);
        Assert.assertNotNull(fileItem);
        Assert.assertNotNull(fileItem.getName());
        Assert.assertEquals(fileItem.getName(), String.format(OUTPUT_FILE_NAME, 0));
        Assert.assertEquals(ExternalSorting.getFileItems().size(), 1);
    }

    @Test
    public void runTest() {
        List<LineEntry> list = new ArrayList<>();
        list.add(new LineEntry(new String[]{"bbc", "val1"}));
        list.add(new LineEntry(new String[]{"abc", "val1"}));
        ExternalSorting.setFileItems(new ArrayList<>());
        SortPartTask sortPartTask = new SortPartTask(list);
        sortPartTask.run();

        CSVReader csvReader = new CSVReader(String.format(OUTPUT_FILE_NAME, 0));
        List<LineEntry> result = csvReader.readForSingleThread(list.size());
        csvReader.close();
        File file = new File(OUTPUT_FILE_NAME);
        file.delete();
        Assert.assertEquals(result.size(), list.size());
    }
}
