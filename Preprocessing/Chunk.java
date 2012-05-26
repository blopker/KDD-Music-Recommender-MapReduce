/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package Preprocessing;

import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author sarah
 */
public class Chunk {
    private FileWriter fileWriter;
    private BufferedWriter out;
    private ArrayList<String> currentUserSongRatings;
    private String filename;
    private boolean shouldPrint;
    
    public Chunk(String filename) {
        try {
            fileWriter = new FileWriter(filename);
            out = new BufferedWriter(fileWriter);
        } catch (IOException e) {
            System.err.println("Failed to create chunk for file: " + filename);
            System.exit(1);
        }
        currentUserSongRatings = new ArrayList();
        shouldPrint = false;
    }
    
    public void addSongRating(String songAndRating) {
        currentUserSongRatings.add(songAndRating);
        shouldPrint = true;
    }
    
    public void printSongs(String userLine) {
        if (!shouldPrint)
            return;

        try {
            out.write(userLine);
            out.write("\n");
            for (String songLine : currentUserSongRatings) {
                out.write(songLine);
                out.write("\n");
            }
        } catch (IOException ex) {
            System.err.println("Failed to print song info to chunk " + filename + ": " + userLine + "\n" + ex);
        }
        
        currentUserSongRatings = new ArrayList();
        shouldPrint = false;
    }
    
    public void close() {
        try {
            out.close();
            fileWriter.close();
        } catch (IOException ex) {
            System.err.println("Unable to close files for chunk " + filename + "\n" + ex);
            System.exit(1);
        }
    }
    
}
