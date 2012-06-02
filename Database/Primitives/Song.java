package Database.Primitives;

import Database.Similarities;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 *
 * @author ninj0x
 */
public class Song implements Writable, WritableComparable<Song> {

    private int id;
    private int totalRating;
    private int ratingCount;
    private Similarities similarities = new Similarities();

    public Song(int id, int rating) {
        this.id = id;
        this.totalRating = rating;
        ratingCount = 1;
    }

    public Song(int id) {
        this.id = id;
        this.totalRating = 0;
        ratingCount = 0;
    }

    public void addRating(int rating) {
        totalRating += rating;
        ratingCount++;
    }

    public void joinRating(int rating, int count) {
        totalRating += rating;
        ratingCount += count;
    }

    public int getRatingCount() {
        return ratingCount;
    }

    /**
     * Avgerage Rating is getRating()/getRatingCount()
     * @return
     */
    public int getRating() {
        return totalRating;
    }

    public int getID() {
        return id;
    }

    public Similarities getNeighborhood() {
        if (similarities == null) {
            similarities = new Similarities();
        }
        return similarities;
    }

    public void addToNeighborhood(Similarity sim) {
        if (similarities == null) {
            similarities = new Similarities();
        }
        similarities.insert(sim);
    }

    public double getSimilarity(Song s) {
        if (similarities == null) {
            similarities = new Similarities();
        }
        return similarities.getSimilarity(s);
    }

    public void print() {
        if (similarities == null) {
            similarities = new Similarities();
        }
        System.out.println(id);
        similarities.print();
    }

    @Override
    public boolean equals(Object o) {
    if (o instanceof Song) {
      Song other = (Song) o;
      return (id == other.id);
    }
    return false;
  }

    @Override
    /**
     * We only write out the ID because the value will be the neighborhood,
     * which is written as an Iterator<Song>
     */
    public void write(DataOutput d) throws IOException {
        d.writeInt(id);
    }

    @Override
    public void readFields(DataInput di) throws IOException {
        id = di.readInt();
    }


    public int compareTo(Song o) {
        return id - o.getID();
    }
    }
