package Database;

import java.io.*;
import java.net.URI;
import java.util.Scanner;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

public abstract class Parser {

    Songs songs;
    Users users;
    private BufferedReader file;
    Scanner scanner;
//    private FileReader reader;
    
    /**
     * Constructor for local files.
     * @param database_file 
     */
    public Parser(String database_file) {
        try {
            file = new BufferedReader(new FileReader(database_file));
            scanner = new Scanner(file);
        } catch (FileNotFoundException e) {
            System.err.println("Unable to open database: " + database_file);
            e.printStackTrace();
        }
    }
    
    /**
     * Constructor for HDFS files.
     * @param database_file
     * @param conf 
     */
    public Parser(String database_file, Configuration conf) {
        try {
            FileSystem fs = FileSystem.get(URI.create(database_file), conf);
            InputStream in = fs.open(new Path(database_file));
            scanner = new Scanner(in);
        } catch (IOException ex) {
            Logger.getLogger(Parser.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    public void parse(Songs s, Users u) {
        songs = s;
        users = u;
        System.err.println("Reading database...");
//        Scanner scanner = new Scanner(file);
        while (scanner.hasNextLine()) {
            format(scanner.nextLine());
        }
        System.err.println("Database loaded.");
        close();
    }
    
    public void close() {
        try {
            file.close();
            scanner.close();
        } catch (IOException ex) {
            System.err.println("Unable to close " + file.toString());
            ex.printStackTrace();
        }
    }

    protected abstract void format(String dataLine);
}
