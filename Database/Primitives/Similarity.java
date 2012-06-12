package Database.Primitives;

/**
 *
 * @author sarahejones, sns
 */
public class Similarity implements Comparable<Similarity>{

    private Song neighbor;
    private double similarity;

    public Similarity(Song neighbor, double s) {
        this.neighbor = neighbor;
        similarity = s;
    }

    public Similarity(String idAndValue) {
        String[] split = idAndValue.split(" ");
        this.neighbor = new Song(Integer.valueOf(split[0]));
        similarity = Double.valueOf(split[1]);
    }

    public double getSimilarity() {
        return similarity;
    }

    public Song getNeighborSong() {
        return neighbor;
    }

    @Override
    public int compareTo(Similarity s) {
        double sim_other = s.getSimilarity();
        if (similarity > sim_other) {
            return 1;
        } else {
            return -1;
        }
    }

    @Override
    public String toString() {
        return neighbor.getID() + " " + similarity;
    }
}
