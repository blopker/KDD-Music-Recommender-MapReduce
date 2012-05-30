/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package Recommender;

import Database.Primitives.User;
import Database.Songs;
import Main.Main;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author ninj0x
 */
public class ParallelKNN implements Recommender {

    @Override
    public void createNeighborhoods() {
        try {
            WordCount.run(Main.getOptions().getArgumentList());
        } catch (IOException ex) {
            Logger.getLogger(ParallelKNN.class.getName()).log(Level.SEVERE, null, ex);
        } catch (InterruptedException ex) {
            Logger.getLogger(ParallelKNN.class.getName()).log(Level.SEVERE, null, ex);
        } catch (ClassNotFoundException ex) {
            Logger.getLogger(ParallelKNN.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    @Override
    public void recommendSong(User active, Songs songs, double threshold) {
        throw new UnsupportedOperationException("Not supported yet.");
    }
}
