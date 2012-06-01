/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package Main;

import java.util.ArrayList;
import java.util.List;
import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.Option;

/**
 *
 * @author ninj0x
 */
public class KNNOptions {
    public static final String USAGE = "Usage:\njava -jar KDD-Music-Recommender.jar -k N -r <ratingCountThreshold> DATABASE\n"
            + "KDD-Music-Recommender.jar -q -t D -n NEIGHBOR_FILE DATABASE\n"
            + "KDD-Music-Recommender.jar -pre <inFile> <outFilePrefix> <numberOfChunks> <numberOfSongs>\n";
    
    public enum Mode{
        CALC, QUERY, PRE, PARALLEL;
    }
    
    @Option(name="-k",usage="How many nearest neighbors to calculate.")
    private int k = 10;

    @Option(name="-q",usage="Activate query mode!!!")
    private boolean query = false;
    
    @Option(name="-pre",usage="Activate preprocess mode!!!")
    private boolean preprocess = false;
    
    @Option(name="-p",usage="Activate parallel mode!!!")
    private boolean parallel = false;

    @Option(name="-n", usage="Neighborhood file, used for query only")
    private String  neighborhood_file;
    
    @Option(name="-t", usage="Predicted rating threshold (100 to -100), used for query only")
    private double  threshold;
    
    @Option(name="-r", usage="Rating count threshold, used for calc mode only")
    private int ratingCountThreshold = 1;
    
    // receives other command line parameters than options
    @Argument
    private List<String> arguments = new ArrayList<String>();

    public Mode getMode(){
        // Hack, probably a better way to do this.
        Mode mode = Mode.CALC;
        mode = (query)?Mode.QUERY:mode;
        mode = (preprocess)?Mode.PRE:mode;
        mode = (parallel)?Mode.PARALLEL:mode;
        return mode;
    }
    
    public String getDatabasePath() {
        return arguments.get(0);
    }
    
    public List<String> getArgumentList() {
        return arguments;
    }

    public int getK() {
        return k;
    }

    public String getNeighborhoodFilePath() {
        return neighborhood_file;
    }

    public double getThreshold() {
        return threshold;
    }
    
    public int getRatingCountThreshold() {
        return ratingCountThreshold;
    }
    
}
