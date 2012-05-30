package Main;

import Database.Primitives.User;
import Database.*;
import Preprocessing.Preprocessing;
import Recommender.ParallelKNN;
import Recommender.Recommender;
import Recommender.SequentialKNN;
import java.util.List;
import java.util.Scanner;
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
        System.out.println(options.USAGE);
        parser.printUsage(System.out);
        System.exit(1);
    }

    private static void run() {
        Recommender recommender = getRecommender();
        // What mode are we in?
        switch (options.getMode()) {
            case CALC:
                System.out.println("hi");
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
        return options.isParallel()?new ParallelKNN():new SequentialKNN();
    }

    private static void calculate(Recommender recommender) {
        recommender.createNeighborhoods();
    }

    private static void query(Recommender recommender) {
        Songs songs = new Songs();
        Users users = new Users();

        Parser kddParser = new KDDParser(options.getDatabasePath());
        kddParser.parse(songs, users);

        Parser nbrParser = new NeighborhoodParser(options.getNeighborhoodFilePath());  //alternatively print out users that rated that item
        nbrParser.parse(songs, users);

        Scanner in = new Scanner(System.in);
        int line;
        System.out.println("Enter user id");

        while (in.hasNext()) {
            line = Integer.parseInt(in.nextLine());
            User u = users.getUser(line);
            if (u == null) {
                System.out.println("Invalid user id");
                continue;
            }
            recommender.recommendSong(u, songs,options.getThreshold());
            System.out.println("Enter user id");
        }
        throw new UnsupportedOperationException("Not yet implemented");
    }

    private static void preprocess() {
        List<String> list = options.getArgumentList();
        Preprocessing pp = new Preprocessing(list.get(0), list.get(1), Integer.parseInt(list.get(2)), Integer.parseInt(list.get(3)));
        pp.parse();
        pp.close();
    }
}
