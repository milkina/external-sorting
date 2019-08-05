package sort.io;

import sort.controller.ExternalSorting;
import sort.model.LineEntry;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class CSVReader implements Closeable {
    private static final String DELIMITER = ",";
    private BufferedReader br;
    private String fileName;

    public CSVReader(String fileName) {
        this.fileName = fileName;
        try {
            this.br = new BufferedReader(new FileReader(fileName));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    /**
     * Read from file not more than maxLines rows. If oneThread==false, synchronization is in work.
     * The current Thread is waiting until rows in the memory <=maxRows.
     *
     * @param maxLines    - number of rows to read
     * @param isOneThread - flag for oneThread read.
     * @return List<LineEntry> read rows
     */
    public List<LineEntry> read(int maxLines, boolean isOneThread) {
        String line;
        int i = 0;
        List<LineEntry> list = new ArrayList<>();
        try {
            while (i++ < maxLines) {
                synchronized (ExternalSorting.class) {
                    while (ExternalSorting.memoryRows == maxLines && !isOneThread) {
                        ExternalSorting.class.wait();
                    }
                    line = br.readLine();
                    if (line == null) {
                        break;
                    }
                    String[] columns = line.split(DELIMITER);
                    LineEntry lineEntry = new LineEntry(columns);
                    list.add(lineEntry);
                    if (!isOneThread) {
                        ExternalSorting.memoryRows++;
                    }
                }
            }
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
        return list;
    }

    /**
     * Read from file not more than maxLines rows.
     *
     * @param maxLines - number of rows to read
     * @return List<LineEntry> read rows
     */
    public List<LineEntry> read(int maxLines) {
        return read(maxLines, true);
    }

    /**
     * Read one row from CSV file by number lineIndex
     *
     * @param lineIndex number of row to read
     * @return LineEntry
     */
    public LineEntry readLine(int lineIndex) {
        String line = "";
        int i = 0;
        try {
            while (i++ < lineIndex) {
                line = br.readLine();
                if (line == null) {
                    return null;
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        String[] columns = line.split(DELIMITER);
        return new LineEntry(columns);
    }

    @Override
    public void close() {
        try {
            if (br != null) {
                br.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
