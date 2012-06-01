/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package Recommender;

import Database.Primitives.User;
import Database.Songs;
import java.io.FileNotFoundException;


/**
 *
 * @author ninj0x
 */
public interface Recommender {
    public void createNeighborhoods();
    public void recommendSong(String activeUserFile, double threshold) throws FileNotFoundException;
}
