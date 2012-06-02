/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package Database.Primitives;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;

/**
 *
 * @author sarah
 */
public class PredictedRating implements Writable {

    Song song;
    double predictedRating;
    
    public PredictedRating(Song s, double predictedRating) {
        song = s;
        this.predictedRating = predictedRating;
    }
    
    @Override
    public void write(DataOutput d) throws IOException {
        d.writeInt(song.getID());
        d.writeDouble(predictedRating);
    }

    @Override
    public void readFields(DataInput di) throws IOException {
        throw new UnsupportedOperationException("Not supported yet.");
    }
    
    
}
