package sort.controller;

import sort.io.CSVWriter;
import sort.model.FileItem;
import sort.model.LineEntry;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

public class SortPartTask implements Runnable {
    public static final String OUTPUT_FILE_NAME = "csv\\tmp\\output_%s.csv";
    private List<LineEntry> list;

    public SortPartTask(List<LineEntry> list) {
        this.list = list;
    }

    public void run() {
        Collections.sort(list);
        FileItem fileItem = generateTmpFileItem(list.size());
        try (CSVWriter csvWriter = new CSVWriter(fileItem.getName())) {
            csvWriter.write(list);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Create FileItem for the next temporary File
     *
     * @param rowNumber number of rows in the file
     * @return FileItem
     */
    public FileItem generateTmpFileItem(int rowNumber) {
        FileItem fileItem;
        synchronized (ExternalSorting.class) {
            List<FileItem> fileItems = ExternalSorting.getFileItems();
            String fileName = String.format(OUTPUT_FILE_NAME, fileItems.size());
            fileItem = new FileItem(rowNumber, fileName);
            fileItems.add(fileItem);
        }
        return fileItem;
    }
}
