package sort.io;

import sort.model.LineEntry;

import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

public class CSVWriter implements Closeable {
    public static final String DELIMITER = ";";
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

    public void write(List<LineEntry> list) throws IOException {
        for (LineEntry lineEntry : list) {
            write(lineEntry);
        }
    }

    public void write(LineEntry lineEntry) throws IOException {
        for (String column : lineEntry.getColumns()) {
            br.write(column);
            br.write(DELIMITER);
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
