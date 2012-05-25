/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package preprocessing;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Scanner;

/**
 *
 * @author sarah
 */
public class Preprocessing {

    private int numberOfChunks;
    private String outFilePrefix;
    private int numberOfSongsPerChunk;
    private String inFile;
    private Scanner scan;
    private FileInputStream file;
    private String currentUserLine;
    private int currentUserRatingCount;
    private int currentUserRatingSum;
    private Chunk[] chunks;
    
    public Preprocessing(String inFile, String outFilePrefix, int numberOfChunks, int numberOfSongs) {
        this.inFile = inFile;
        this.outFilePrefix = outFilePrefix;
        this.numberOfChunks = numberOfChunks;
        this.numberOfSongsPerChunk = (int) Math.ceil(numberOfSongs/((double)numberOfChunks));
        chunks = new Chunk[numberOfChunks];
        
        for (int i = 0; i < numberOfChunks; i++)
            chunks[i] = new Chunk(outFilePrefix + i);
        
        try {
            file = new FileInputStream(inFile);
        } catch (FileNotFoundException e) {
            System.err.println("Database file " + inFile + " not found.");
            System.exit(1);
        }        
        scan = new Scanner(file);
        
    }
    
    public void close() {
        scan.close();
        try {
            file.close();
        } catch (IOException ex) {
            System.err.println("Unable to close database file " + inFile + "\n" + ex);
            System.exit(1);
        }
    }
    
    
    public void parse() {
        while (scan.hasNext()) {
            String line = scan.next();
            if (line.contains("|")) {
                printChunks(currentUserLine + "|" + currentUserAverageRating());
                currentUserLine = line;
                currentUserRatingCount = 0;
                currentUserRatingSum = 0;
            } else {
                int[] songAndRating = strArrayToIntArray(line.split("\t"));
                chunks[songAndRating[0] / numberOfSongsPerChunk].addSongRating(line);
                currentUserRatingSum += songAndRating[1];
                currentUserRatingCount++;
            }

        }

    }
     
    private static int[] strArrayToIntArray(String[] line) {
        int[] infoLine = new int[line.length];
        for (int i = 0; i < line.length; i++) {
            int info = (line[i].toLowerCase() == "none") ? -1 : Integer.valueOf(line[i]).intValue();
            infoLine[i] = info;
        }
        return infoLine;
    }
    
    private double currentUserAverageRating() {
        return ((double)currentUserRatingSum) / currentUserRatingCount;
        
    }
    
    private void printChunks(String userLine) {
        for (Chunk chunk : chunks) {
            chunk.printSongs(userLine);
        }
    }
}
