package Database;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Scanner;
import java.util.logging.Level;
import java.util.logging.Logger;

public abstract class Parser {

    Songs songs;
    Users users;
    private BufferedReader file;
    private FileReader reader;
    

    public Parser(String database_file) {
        try {
            reader = new FileReader(database_file);
            file = new BufferedReader(reader);
        } catch (FileNotFoundException e) {
            System.err.println("Unable to open database: " + database_file);
            e.printStackTrace();
        }
    }

    public void parse(Songs s, Users u) {
        songs = s;
        users = u;
        System.err.println("Reading database...");
        StringBuilder text = new StringBuilder();
        Scanner scanner = new Scanner(file);
        while (scanner.hasNextLine()) {
            format(scanner.nextLine());
        }
        scanner.close();
        System.err.println("Database loaded.");
    }
    
    public void close() {
        try {
            file.close();
            reader.close();
        } catch (IOException ex) {
            System.err.println("Unable to close " + file.toString() + " or " + reader.toString());
            ex.printStackTrace();
        }
    }

    protected abstract void format(String dataLine);
}
