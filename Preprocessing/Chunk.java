/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package preprocessing;

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
    BufferedWriter out;
    ArrayList<String> currentUserSongRatings;
    String filename;
    boolean shouldPrint;
    
    public Chunk(String filename) {
        try {
            out = new BufferedWriter(new FileWriter(filename));
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
            for (String songLine : currentUserSongRatings)
                out.write(songLine);
        } catch (IOException ex) {
            System.err.println("Failed to print song info to chunk " + filename + ": " + userLine + "\n" + ex);
        }
        
        currentUserSongRatings = new ArrayList();
        shouldPrint = false;
    }
    
}
