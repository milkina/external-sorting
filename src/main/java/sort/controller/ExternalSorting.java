package sort.controller;

import sort.dao.LineEntryDAO;
import sort.io.CSVReader;
import sort.io.CSVWriter;
import sort.model.FileItem;
import sort.model.LineEntry;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.ForkJoinPool;

public class ExternalSorting {
    private static final String RESULT_FILE_NAME = "csv\\result.csv";
    private String fileName;
    private int maxRows;

    public ExternalSorting(String fileName, int keyIndex, int maxRows) {
        this.fileName = fileName;
        this.maxRows = maxRows;
        LineEntry.setKeyIndex(keyIndex);
        SortPartTask.setMaxRows(maxRows);
    }

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public int getMaxRows() {
        return maxRows;
    }

    public void setMaxRows(int maxRows) {
        this.maxRows = maxRows;
    }

    /**
     * Sort entries in a CSV file
     */
    public void sort() {
        List<FileItem> fileItems = sortParts();
        mergeParts(fileItems);
    }

    /**
     * Merge sorted parts into one file
     *
     * @param fileItems
     */
    private void mergeParts(List<FileItem> fileItems) {
        LineEntry minLineEntry;
        try (CSVWriter csvWriter = new CSVWriter(RESULT_FILE_NAME)) {
            while ((minLineEntry = findMinLineEntry(fileItems)) != null) {
                csvWriter.write(minLineEntry);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println("Results are successfully stored in " + RESULT_FILE_NAME);
    }

    /**
     * Write sorted data from CSV to DB by maxRows chunks
     */
    public void writeToDB() {
        LineEntryDAO lineEntryDAO = new LineEntryDAO();
        try (CSVReader csvReader = new CSVReader(RESULT_FILE_NAME)) {
            List<LineEntry> list;
            boolean isTableCreated = false;
            String tableName = null;
            while ((list = csvReader.read(maxRows)) != null && !list.isEmpty()) {
                if (!isTableCreated) {
                    tableName = lineEntryDAO.createTable(list.get(0));
                    isTableCreated = true;
                }
                lineEntryDAO.createEntities(list, tableName);
            }
        }
        lineEntryDAO.close();
        System.out.println("Results are successfully written to DB.");
    }

    /**
     * Find minimal entry from sorted files and remove it from further searching
     *
     * @param fileItems
     * @return minimal entry
     */
    private LineEntry findMinLineEntry(List<FileItem> fileItems) {
        LineEntry minLineEntry = null;
        FileItem minFileItem = null;
        for (FileItem fileItem : fileItems) {
            try (CSVReader csvReader = new CSVReader(fileItem.getName())) {
                LineEntry lineEntry = csvReader.readLine(fileItem.getCurrentRow() + 1);
                if (minLineEntry == null || lineEntry.compareTo(minLineEntry) < 0) {
                    minLineEntry = lineEntry;
                    minFileItem = fileItem;
                }
            }
        }
        if (minFileItem != null) {
            minFileItem.increaseCurrentRow();
            if (minFileItem.getCurrentRow() == minFileItem.getRowNumber()) {
                fileItems.remove(minFileItem);
                File file = new File(minFileItem.getName());
                file.delete();
            }
        }
        return minLineEntry;
    }

    /**
     * Read chunks of rows from CSV file, sort them and store in the files
     *
     * @return list of created files
     */
    private List<FileItem> sortParts() {
        ForkJoinPool pool = new ForkJoinPool();
        SortPartTask.setCsvReader(fileName);
        SortPartTask sortPartTask = new SortPartTask();
        pool.invoke(sortPartTask);
        return SortPartTask.getFileItems();
    }
}
