package Recommender;

import java.io.FileNotFoundException;


/**
 *
 * @author Karl Lopker
 */
public interface Recommender {
    public void createNeighborhoods();
    public void recommendSong(String activeUserFile, double threshold) throws FileNotFoundException;
}
