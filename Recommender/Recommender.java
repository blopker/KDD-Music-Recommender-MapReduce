/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package Recommender;

import Database.Primitives.User;
import Database.Songs;


/**
 *
 * @author ninj0x
 */
public interface Recommender {
    public void createNeighborhoods();
    public void recommendSong(User active, Songs songs, double threshold);
}
