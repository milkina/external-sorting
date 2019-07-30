package sort;

import sort.io.CSVReader;
import sort.io.CSVWriter;
import sort.dao.LineEntryDAO;
import sort.model.FileItem;
import sort.model.LineEntry;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class Main {

    public static final String OUTPUT_FILE_NAME = "csv\\output_%s.csv";
    public static final String RESULT_FILE_NAME = "csv\\result.csv";

    public static void main(String[] args) {
        int maxRows = 3;
        int keyIndex = 2;
        LineEntry.setKeyIndex(keyIndex);
        sort(maxRows);
        writeToDB(maxRows);
    }

    private static void sort(int maxRows) {
        List<FileItem> fileItems = sortParts(maxRows);
        mergeParts(fileItems);
    }

    private static void mergeParts(List<FileItem> fileItems) {
        LineEntry minLineEntry = null;
        try (CSVWriter csvWriter = new CSVWriter(RESULT_FILE_NAME)) {
            while ((minLineEntry = findMinLineEntry(fileItems)) != null) {
                System.out.println(minLineEntry);
                csvWriter.write(minLineEntry);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void writeToDB(int maxRows) {
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
        } catch (IOException exception) {
            System.out.println(exception.getMessage());
        }
        lineEntryDAO.close();
    }

    private static LineEntry findMinLineEntry(List<FileItem> fileItems) {
        LineEntry minLineEntry = null;
        FileItem minFileItem = null;
        for (FileItem fileItem : fileItems) {
            try (CSVReader csvReader = new CSVReader(fileItem.getName())) {
                LineEntry lineEntry = csvReader.readLine(fileItem.getCurrentRow() + 1);
                if (minLineEntry == null || lineEntry.compareTo(minLineEntry) < 0) {
                    minLineEntry = lineEntry;
                    minFileItem = fileItem;
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        if (minFileItem != null) {
            minFileItem.increaseCurrentRow();
            if (minFileItem.getCurrentRow() == minFileItem.getRowNumber()) {
                fileItems.remove(minFileItem);
            }
        }
        return minLineEntry;
    }

    private static List<FileItem> sortParts(int maxRows) {
        int fileNumber = 0;
        List<LineEntry> list;
        List<FileItem> fileItems = new ArrayList<>();
        try (CSVReader csvReader = new CSVReader("csv\\a.csv")) {
            while (!(list = csvReader.read(maxRows)).isEmpty()) {
                Collections.sort(list);
                FileItem fileItem = createFile(list, fileNumber++);
                fileItems.add(fileItem);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return fileItems;
    }

    private static FileItem createFile(List<LineEntry> list, int fileNumber) {
        String fileName = String.format(OUTPUT_FILE_NAME, fileNumber);
        try (CSVWriter csvWriter = new CSVWriter(fileName)) {
            csvWriter.write(list);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return new FileItem(list.size(), fileName);
    }
}
