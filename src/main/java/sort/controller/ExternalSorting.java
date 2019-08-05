package sort.controller;

import sort.dao.LineEntryDAO;
import sort.io.CSVReader;
import sort.io.CSVWriter;
import sort.model.FileItem;
import sort.model.LineEntry;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class ExternalSorting {
    private static final String RESULT_FILE_NAME = "csv\\result.csv";
    private String fileName;
    private int maxRows;
    private static List<FileItem> fileItems;
    public static volatile int memoryRows;

    public ExternalSorting(String fileName, int keyIndex, int maxRows) {
        this.fileName = fileName;
        this.maxRows = maxRows;
        LineEntry.setKeyIndex(keyIndex);
        fileItems = new ArrayList<>();
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

    public static List<FileItem> getFileItems() {
        return fileItems;
    }

    public static void setFileItems(List<FileItem> fileItems) {
        ExternalSorting.fileItems = fileItems;
    }

    /**
     * Sort entries in a CSV file
     */
    public void sort() {
        sortParts();
        mergeParts();
    }

    /**
     * Merge sorted parts into one file
     */
    private void mergeParts() {
        LineEntry minLineEntry;
        try (CSVWriter csvWriter = new CSVWriter(RESULT_FILE_NAME)) {
            while ((minLineEntry = findMinLineEntry()) != null) {
                csvWriter.write(minLineEntry);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println("Results are successfully stored in " + RESULT_FILE_NAME);
    }

    /**
     * Write sorted data from CSV to DB by maxRows chunks
     *
     * @return name of created Table
     */
    public String writeToDB() {
        LineEntryDAO lineEntryDAO = new LineEntryDAO();
        lineEntryDAO.openConnection();
        String tableName = null;
        try (CSVReader csvReader = new CSVReader(RESULT_FILE_NAME)) {
            List<LineEntry> list;
            while ((list = csvReader.read(maxRows)) != null && !list.isEmpty()) {
                if (tableName == null && list.get(0) != null && list.get(0).getColumns() != null) {
                    tableName = lineEntryDAO.createTable(list.get(0).getColumns().length);
                }
                lineEntryDAO.createEntities(list, tableName);
            }
        }
        lineEntryDAO.close();
        System.out.println("Results are successfully written to table." + tableName);
        return tableName;
    }

    /**
     * Find minimal entry from sorted files and remove it from further searching
     *
     * @return minimal entry
     */
    public LineEntry findMinLineEntry() {
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
        increaseCurrentRow(minFileItem);
        return minLineEntry;
    }

    /**
     * Move cursor to the next row in the temporary file to prevent its repeated usage
     *
     * @param minFileItem file with minimal row in current iteration
     */
    public void increaseCurrentRow(FileItem minFileItem) {
        if (minFileItem != null) {
            minFileItem.increaseCurrentRow();
            if (minFileItem.getCurrentRow() == minFileItem.getRowNumber()) {
                removeFileItem(fileItems, minFileItem);
            }
        }
    }

    /**
     * Remove File from the list and from the disk
     *
     * @param fileItems
     * @param minFileItem
     */
    private void removeFileItem(List<FileItem> fileItems, FileItem minFileItem) {
        fileItems.remove(minFileItem);
        File file = new File(minFileItem.getName());
        file.delete();
    }

    /**
     * Read chunks of rows from CSV file, sort them in different threads and store in the files
     */
    private void sortParts() {
        ExecutorService executorService =
                Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
        try (CSVReader csvReader = new CSVReader(fileName)) {
            List<LineEntry> list;
            while ((list = csvReader.read(maxRows, false)) != null && !list.isEmpty()) {
                executorService.submit(new SortPartTask(list));
            }
            executorService.shutdown();
            executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
            System.out.println(e.getMessage());
        }
    }
}
