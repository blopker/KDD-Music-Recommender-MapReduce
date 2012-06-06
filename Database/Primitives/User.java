package Database.Primitives;

import Database.Songs;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;

/**
 *
 * @author ninj0x
 */
public class User implements Writable {
    private Songs ratings;
    private int id;  //final
    private int sumRatings = 0;
    private double avgRatingChunk = -1;
    private double chunkRating = 0;
    
    public User(int id){
        this.id = id;
        ratings = new Songs();
    }
    
    public int getID(){
        return id;
    }
    
    public Songs getRatings(){
        return ratings;
    }
    
    public int getRating(Song song){
        return getRating(song.getID());
        
    }
    
    public int getRating(int id){
        return ratings.getSong(id).getRating();
    }
    
    public void addRating(Song song){
        ratings.addSong(song);
        sumRatings += song.getRating();
    }
    
    public double getAvgRating(){
        if(avgRatingChunk == -1){
            return ((double)sumRatings)/ratings.getCount();
        } else {
            return avgRatingChunk;
        }        
    }
    
    public void setAvgRating(double rating){
        avgRatingChunk = rating;
    }
    
    public boolean rated(Song song){
        return rated(song.getID());
    }
    
    public boolean rated(int id){
        return ratings.containsSong(id);
    }

    @Override
    public void write(DataOutput d) throws IOException {
        d.writeInt(id);
    }

    @Override
    public void readFields(DataInput di) throws IOException {
        id = di.readInt();
    }
    
}
