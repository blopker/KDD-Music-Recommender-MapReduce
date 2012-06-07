/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package Recommender;

import Database.*;
import Database.Primitives.Similarity;
import Database.Primitives.Song;
import Database.Primitives.User;
import Main.Main;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Scanner;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 *
 * @author ninj0x
 */
public class ParallelKNN extends Configured implements Recommender {

    final static String CHUNK_LIST_NAME = "chunkNameList.txt";
    final static String MY_CHUNK_NAME_ID = "mychunk";
    final static String THRESHOLD = "threshold";

    @Override
    public void createNeighborhoods() {
        FileStatus[] chunks;
        try {
            Configuration conf = new Configuration();
            FileSystem fs = FileSystem.get(conf);
            Path chunkDir = new Path(Main.getOptions().getArgumentList().get(0));
            chunks = fs.listStatus(chunkDir);
//            runCalc(chunks[0].getPath(), chunks);
            for (FileStatus chunk : chunks) {
//                System.out.println(chunk.getPath().toString());
                runCalc(chunk.getPath(), chunks);
            }
        } catch (IOException ex) {
            Logger.getLogger(ParallelKNN.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    @Override
    public void recommendSong(String activeUserFile, double threshold) throws FileNotFoundException {
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
//            System.out.println("\nRecommendations for user:" + active.getID());
//      forall items i
            for (Song s : songs) {
//        forall neighborhood_i Union items_rated_by_user(active) item
                double numerator = 0, denominator = 0, predictedRating;
                for (Song ratedByActive : active.getRatings()) {
                    if (s.getNeighborhood().contains(ratedByActive) && !active.rated(s)) {
                        double similarity = songs.getSong(ratedByActive.getID()).getSimilarity(s);
                        System.out.println("Sim" + similarity);
//                numerator += math ... similarity(i, item) * active.rating(item) …
                        numerator += similarity * active.getRating(ratedByActive);
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

    /**
     * *************************************************************************
     * Calculate Neighborhoods
     */
    /**
     * Map Class for Calculating Neighbors Input: KDD Database in Preprocessed
     * chunks based on Song Output: Neighborhoods for all songs Compare chunks
     * to each other and reduce to get the top K neighbors
     */
    public static class CalcMap
            extends Mapper<Object, Text, IntWritable, Text> {

        private Users myUsers = new Users();
        private Songs mySongs = new Songs();
        private int threshold;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            threshold = context.getConfiguration().getInt(THRESHOLD, 1);
            Path[] cachedFiles = DistributedCache.getLocalCacheFiles(context.getConfiguration());
            String myChunkName = context.getConfiguration().get(MY_CHUNK_NAME_ID);
            for (Path file : cachedFiles) {
                if (file.getName().matches(myChunkName)) {
                    KDDParser parser = new KDDParser(file.toString());
                    parser.parse(mySongs, myUsers);
                }
            }
        }

        @Override
        public void map(Object key, Text chunkURI, Context context) throws IOException, InterruptedException {

            Users otherUsers = new Users();
            Songs otherSongs = new Songs();
            KDDParser parser = new KDDParser(chunkURI.toString(), context.getConfiguration());
            parser.parse(otherSongs, otherUsers);


            //forall items i  //ith iteration
            for (Song i : mySongs) {

                //    forall items j  //split this into N parts
                for (Song j : otherSongs) {
                    double numerator = 0, denominator_left = 0, denominator_right = 0;

                    if (j.equals(i)) {
                        continue;
                    }

                    //        forall users user
                    int userCount = 0;
                    for (User user : myUsers) {
                        double num = 0, den_l = 0, den_r = 0;
//                        System.out.println("comparing! song: " + i.getID() + " and " + j.getID());
                        User other = otherUsers.getUser(user.getID());
                        if (other == null) {
                            continue;
                        }
                        if (user.rated(i) && other.rated(j)) {
//                            System.out.println("if (user.rated(i) && user.rated(j))! song: " + i.getID() + " and " + j.getID());
                            userCount++;
                            double iTmp = user.getRating(i) - user.getAvgRating();
                            double jTmp = other.getRating(j) - other.getAvgRating();

                            num = iTmp * jTmp;
                            den_l = iTmp * iTmp;
                            den_r = jTmp * jTmp;
                        }
                        numerator += num;
                        denominator_left += den_l;
                        denominator_right += den_r;
                    }
                    /*
                     * sim will equal 1.0 if both songs are rated the same (have
                     * the same difference, for each user) sim will equal -1.0
                     * if the songs have the same difference but different signs
                     * NaN if no users have rated both songs If negative, should
                     * not recommend...
                     */

                    double sim = numerator / Math.sqrt(denominator_left * denominator_right);
                    if (userCount > threshold) {
                        Similarity is = new Similarity(j, sim);
//                        System.out.println(userCount + " " + threshold);
//                        System.out.println("in the map function!song: " + i.getID() + " is: " + is.toString());
                        context.write(new IntWritable(i.getID()), new Text(is.toString()));
                    }
                }
            }
        }
    }

    public static class CalcReduce
            extends Reducer<IntWritable, Text, IntWritable, Text> {

        @Override
        public void reduce(IntWritable songId, Iterable<Text> sim,
                Context context) throws IOException, InterruptedException {
            Similarities neih = new Similarities();
            for (Text s : sim) {
//                System.out.println("sim " + s.toString());
                neih.insert(new Similarity(s.toString()));
            }

            for (Similarity s : neih) {
                context.write(songId, new Text(s.toString()));
            }
        }
    }

    private int runCalc(Path myChunk, FileStatus[] chunks) {
        Configuration conf = new Configuration();

        //need to add MainChunk to DistributedCache
        DistributedCache.addCacheFile(myChunk.toUri(), conf);

        conf.set(MY_CHUNK_NAME_ID, myChunk.getName());
        conf.setInt(THRESHOLD, Main.getOptions().getRatingCountThreshold());
        Job job;
        try {
            job = new Job(conf, "KNNParallelRecommender");
            job.setJarByClass(ParallelKNN.class);
            job.setMapperClass(CalcMap.class);
            job.setCombinerClass(CalcReduce.class);
            job.setReducerClass(CalcReduce.class);
            job.setOutputKeyClass(IntWritable.class);
            job.setOutputValueClass(Text.class);
            FileInputFormat.addInputPath(job, createChunkNameFile(conf, chunks));
            FileOutputFormat.setOutputPath(job, getOutputPath(conf, myChunk));
            try {
                job.submit();
            } catch (InterruptedException ex) {
                Logger.getLogger(ParallelKNN.class.getName()).log(Level.SEVERE, null, ex);
            } catch (ClassNotFoundException ex) {
                Logger.getLogger(ParallelKNN.class.getName()).log(Level.SEVERE, null, ex);
            }
        } catch (IOException ex) {
            Logger.getLogger(ParallelKNN.class.getName()).log(Level.SEVERE, null, ex);
        }


        return 0;
    }

    private Path createChunkNameFile(Configuration conf, FileStatus[] chunks) {
        FileSystem fs;
        Path chuckNameList = new Path(CHUNK_LIST_NAME);
        try {
            fs = FileSystem.get(conf);
            if (fs.exists(chuckNameList)) {
                fs.delete(chuckNameList, true);
            }

            FSDataOutputStream out = fs.create(chuckNameList);

            for (FileStatus chunk : chunks) {
                out.writeBytes(chunk.getPath().toString() + "\n");
            }

            out.close();
        } catch (IOException ex) {
            Logger.getLogger(ParallelKNN.class.getName()).log(Level.SEVERE, null, ex);
        }
        return chuckNameList;
    }

    private Path getOutputPath(Configuration conf, Path chunk) {
        String outputDir = Main.getOptions().getArgumentList().get(1);
        int indexOfExtension = chunk.getName().lastIndexOf(".");
        outputDir += "/";
        outputDir += chunk.getName().substring(0, indexOfExtension);
        Path out = new Path(outputDir);
        removeOldOutputDir(conf, out);
        return out;
    }

    private void removeOldOutputDir(Configuration conf, Path out) {
        try {
            FileSystem fs = FileSystem.get(conf);
            if (fs.exists(out)) {
                fs.delete(out, true);
            }
        } catch (IOException ex) {
            Logger.getLogger(ParallelKNN.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
}
