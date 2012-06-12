package Recommender;

import Database.Primitives.Similarity;
import Database.Primitives.Song;
import Database.Primitives.User;
import Database.*;
import Main.Main;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Scanner;

/**
 *
 * @author sarahejones, sns
 */
public class SequentialKNN implements Recommender {
    /*
     * could precompute this w/ map reduce: finding k nearest neighbors
     */

    @Override
    public void createNeighborhoods() {
        // initalize lists.
        int k = Main.getOptions().getK();
        Parser parser = new KDDParser(Main.getOptions().getDatabasePath());

        Songs songs = new Songs();
        Users users = new Users();
        Calendar loadStartTime = Calendar.getInstance();
        System.err.println("Start Time for loading DB: " + loadStartTime.getTimeInMillis());
        parser.parse(songs, users);
        Calendar loadEndTime = Calendar.getInstance();
        System.err.println("End Time for loading DB: " + loadEndTime.getTimeInMillis());

        
        Calendar runStartTime = Calendar.getInstance();
        System.err.println("Start Time for running KNN algorithm: " + runStartTime.getTimeInMillis());
        //forall items i  //ith iteration
        for (Song i : songs) {

            //    forall items j  //split this into N parts
            for (Song j : songs) {
                double numerator = 0, denominator_left = 0, denominator_right = 0;

                if (j.equals(i)) {
                    continue;
                }

                //        forall users user
                int userCount = 0;
                for (User user : users) {
                    double num = 0, den_l = 0, den_r = 0;
                    if (user.rated(i) && user.rated(j)) {
                        userCount++;
                        double iTmp = user.getRating(i) - user.getAvgRating();
                        double jTmp = user.getRating(j) - user.getAvgRating();

                        num = iTmp * jTmp;
                        den_l = iTmp * iTmp;
                        den_r = jTmp * jTmp;
                    }
                    numerator += num;
                    denominator_left += den_l;
                    denominator_right += den_r;
                }
                /*
                 * sim will equal 1.0 if both songs are rated the same (have the
                 * same difference, for each user) sim will equal -1.0 if the
                 * songs have the same difference but different signs NaN if no
                 * users have rated both songs If negative, should not
                 * recommend...
                 */
                double sim = numerator / Math.sqrt(denominator_left * denominator_right);
                if (userCount > Main.getOptions().getRatingCountThreshold()) {
                    Similarity is = new Similarity(j, sim);
                    i.getNeighborhood().insert(is);
                }

                //attempt to add to neighborhood (it will only be added if it should be)
            }
            i.print();
        }
        Calendar runEndTime = Calendar.getInstance();
        System.err.println("End Time for running KNN algorithm: " + runEndTime.getTimeInMillis());
    }

    @Override
    public void recommendSong(String activeUserFile, double threshold) throws FileNotFoundException{
        Songs songs = new Songs();
        Users users = new Users();
        ArrayList<User> activeUsers = new ArrayList<User>();

        Parser kddParser = new KDDParser(Main.getOptions().getDatabasePath());
        kddParser.parse(songs, users);

        Parser nbrParser = new NeighborhoodParser(Main.getOptions().getNeighborhoodFilePath());  //alternatively print out users that rated that item
        nbrParser.parse(songs, users);
        FileInputStream file = new FileInputStream(activeUserFile);
        Scanner in = new Scanner(file);
        int line;
        User u;
        while (in.hasNext()) {
            line = Integer.parseInt(in.nextLine());
            u = users.getUser(line);
            if (u == null) {
                System.out.println("Invalid user id");
                continue;
            }
           
            activeUsers.add(u);
        }
        
        //recommendation
        for (User active : activeUsers) {
            System.out.println("\nRecommendations for user:" + active.getID());
//      forall items i
            for (Song s : songs) {
//        forall neighborhood_i Union items_rated_by_user(active) item
                double numerator = 0, denominator = 0, predictedRating;
                for (Song ratedByActive : active.getRatings()) {
                    if (s.getNeighborhood().contains(ratedByActive) && !active.rated(s)) {
                        Song rba = songs.getSong(ratedByActive.getID());
                        double similarity = rba.getSimilarity(s);
//                numerator += math ... similarity(i, item) * active.rating(item) …
                        numerator += similarity * active.getRating(rba);
//                denominator += |similarity(i, item)|                    
                        denominator += Math.abs(similarity);
                    }
                }
                predictedRating = numerator / denominator;
                //add item to set of recommended items if preQdicted_rating is “good enough”\
                if (!Double.isNaN(predictedRating)) //if (predictedRating >= threshold)  (Not doing threshold for this part)
                {
                    System.out.println(s.getID() + "\t" + predictedRating);
                }

            }
        }
    }
}
