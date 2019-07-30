package sort.io;

import sort.model.LineEntry;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class CSVReader implements Closeable {
    private static final String DELIMITER = ";";
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
     * Read from file not more than maxLines rows
     *
     * @param maxLines - number of rows to read
     * @return List<LineEntry>
     */
    public List<LineEntry> read(int maxLines) {
        String line = "";
        int i = 0;
        List<LineEntry> list = new ArrayList<>();
        try {
            while (i++ < maxLines) {
                line = br.readLine();
                if (line == null) {
                    break;
                }
                String[] columns = line.split(DELIMITER);
                LineEntry lineEntry = new LineEntry(columns);
                list.add(lineEntry);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return list;
    }

    /**
     * Read one row from CSV file by number lineIndex
     *
     * @param lineIndex number of row to read
     * @return LineEntry
     */
    public LineEntry readLine(int lineIndex) {
        List<LineEntry> list = read(lineIndex);
        if (list.isEmpty()) {
            return null;
        } else {
            return list.get(list.size() - 1);
        }
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
