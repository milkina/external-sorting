package sort.io;

import sort.controller.ExternalSorting;
import sort.model.LineEntry;

import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;

public class CSVWriter implements Closeable {
    private static final String DELIMITER = ",";
    private String fileName;
    private BufferedWriter br;

    public CSVWriter(String fileName) throws IOException {
        this.fileName = fileName;
        this.br = new BufferedWriter(new FileWriter(fileName));
    }

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    /**
     * Write list of entries to CSV file
     *
     * @param list
     * @throws IOException
     */
    public void writeForSingleThread(List<LineEntry> list) throws IOException {
        Iterator<LineEntry> iterator = list.iterator();
        while (iterator.hasNext()) {
            LineEntry lineEntry = iterator.next();
            write(lineEntry);
            iterator.remove();
        }
    }

    /**
     * Synchronized write of list of entries to CSV file.
     *
     * @param list
     * @throws IOException
     */
    public void write(List<LineEntry> list) throws IOException {
        Iterator<LineEntry> iterator = list.iterator();
        while (iterator.hasNext()) {
            LineEntry lineEntry = iterator.next();
            write(lineEntry);
            synchronized (ExternalSorting.class) {
                iterator.remove();
                ExternalSorting.memoryRows--;
                ExternalSorting.class.notifyAll();
            }
        }
    }

    /**
     * Write one entry to CSV File
     *
     * @param lineEntry
     * @throws IOException
     */
    public void write(LineEntry lineEntry) throws IOException {
        String[] columns = lineEntry.getColumns();
        for (int i = 0; i < columns.length; i++) {
            br.write(columns[i]);
            if (i != columns.length - 1) {
                br.write(DELIMITER);
            }
        }
        br.newLine();
    }

    @Override
    public void close() throws IOException {
        if (br != null) {
            br.close();
        }
    }
}
