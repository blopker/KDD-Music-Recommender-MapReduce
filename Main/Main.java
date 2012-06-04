package Main;

import Preprocessing.Preprocessing;
import Recommender.ParallelKNN;
import Recommender.Recommender;
import Recommender.SequentialKNN;
import java.io.FileNotFoundException;
import java.util.List;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;

/*
 * To change this template, choose Tools | Templates and open the template in
 * the editor.
 */
/**
 *
 * @author ninj0x
 */
public class Main {

    private static KNNOptions options = new KNNOptions();

    public static KNNOptions getOptions() {
        return options;
    }

    public static void main(String args[]) {

        // Parse the command line options
        CmdLineParser parser = new CmdLineParser(options);
        try {
            parser.parseArgument(args);
            run();
        } catch (CmdLineException ex) {
            usage(parser);
            commandLineError(ex);
        }
    }

    private static void commandLineError(CmdLineException ex) {
        System.out.println(ex.getMessage());
        CmdLineParser parser = ex.getParser();
        usage(parser);
    }

    private static void usage(CmdLineParser parser) {
        System.out.println(KNNOptions.USAGE);
        parser.printUsage(System.out);
        System.exit(1);
    }

    private static void run() {
        Recommender recommender = getRecommender();
        // What mode are we in?
        switch (options.getMode()) {
            case CALC:
                calculate(recommender);
                break;
            case QUERY:
                query(recommender);
                break;
            case PRE:
                preprocess();
            default:
                break;
        }
    }

    // Dynamically switch recommenders based on mode.
    private static Recommender getRecommender() {
        return options.isParallel() ? new ParallelKNN() : new SequentialKNN();
    }

    private static void calculate(Recommender recommender) {
        recommender.createNeighborhoods();
    }

    private static void query(Recommender recommender) {
        try {
            recommender.recommendSong(options.getActiveUserFile(), options.getThreshold());
        } catch (FileNotFoundException ex) {
            System.err.println("Could not find active user file " + options.getActiveUserFile());
            ex.printStackTrace();
        }
    }

    private static void preprocess() {
        List<String> list = options.getArgumentList();
        Preprocessing pp = new Preprocessing(list.get(0), list.get(1), Integer.parseInt(list.get(2)), Integer.parseInt(list.get(3)));
        pp.parse();
        pp.close();
    }
}
