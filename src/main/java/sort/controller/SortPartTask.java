package sort.controller;

import sort.io.CSVReader;
import sort.io.CSVWriter;
import sort.model.FileItem;
import sort.model.LineEntry;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.RecursiveAction;

public class SortPartTask extends RecursiveAction {
    private static final String OUTPUT_FILE_NAME = "csv\\output_%s.csv";
    private static int maxRows;
    private static int fileNumber;
    private static CSVReader csvReader;
    private static List<FileItem> fileItems = new ArrayList<>();
    private List<LineEntry> list;

    public SortPartTask(List<LineEntry> list) {
        this.list = list;
    }

    public SortPartTask() {
    }

    public static void setMaxRows(int maxRows) {
        SortPartTask.maxRows = maxRows;
    }

    public static List<FileItem> getFileItems() {
        return fileItems;
    }

    public static synchronized void addFileItem(FileItem fileItem) {
        fileItems.add(fileItem);
    }

    public static synchronized int getNextFileNumber() {
        return ++fileNumber;
    }

    public static void setCsvReader(String fileName) {
        SortPartTask.csvReader = new CSVReader(fileName);
        ;
    }

    @Override
    protected void compute() {
        if (list == null) {
            list = csvReader.read(maxRows);
            if (!list.isEmpty()) {
                SortPartTask firstSubTask = new SortPartTask(list);
                SortPartTask secondSubTask = new SortPartTask();
                firstSubTask.fork();
                secondSubTask.compute();
                firstSubTask.join();
            } else {
                csvReader.close();
            }
        } else {
            Collections.sort(list);
            FileItem fileItem = createFile(list);
            addFileItem(fileItem);
        }
    }

    /**
     * Create file with part of sorted string
     *
     * @param list
     * @return FileItem
     */
    private static FileItem createFile(List<LineEntry> list) {
        String fileName = String.format(OUTPUT_FILE_NAME, getNextFileNumber());
        try (CSVWriter csvWriter = new CSVWriter(fileName)) {
            csvWriter.write(list);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return new FileItem(list.size(), fileName);
    }
}
