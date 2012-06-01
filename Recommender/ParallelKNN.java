/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package Recommender;

import Database.KDDParser;
import Database.Primitives.Similarity;
import Database.Primitives.Song;
import Database.Primitives.User;
import Database.Songs;
import Database.Users;
import Main.Main;
import java.io.IOException;
import java.util.Iterator;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.StringUtils;

/**
 *
 * @author ninj0x
 */
public class ParallelKNN extends Configured implements Recommender {

    @Override
    public void createNeighborhoods() {
        FileStatus[] chunks;
        try {
            Configuration conf = new Configuration();
            FileSystem fs = FileSystem.get(conf);
            Path chunkDir = new Path(Main.getOptions().getArgumentList().get(0));
            chunks = fs.listStatus(chunkDir);

            for (FileStatus chunk : chunks) {
//                System.out.println(chunk.getPath().toString());
                run(chunk.getPath(), chunks);
            }
        } catch (IOException ex) {
            Logger.getLogger(ParallelKNN.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    @Override
    public void recommendSong(User active, Songs songs, double threshold) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    //ChunkObject -> ChunkNeighborhoods
    public static class Map extends MapReduceBase implements Mapper<Text, Text, Song, Iterator<Similarity>> {

        private Songs mainSongs;    //my chunk Songs
        private Users mainUsers;    //my chunk Users  

        @Override
        public void configure(JobConf job) {
            //get the mainChunk (from cache)
            Path[] localFiles = new Path[0];
            try {
                localFiles = DistributedCache.getLocalCacheFiles(job);
            } catch (IOException ioe) {
                System.err.println("Caught exception while getting cached files: " + StringUtils.stringifyException(ioe));
            }
            parseMainChunk(localFiles[0]);
        }

        private void parseMainChunk(Path mainChunkFilename) {

            KDDParser parser = new KDDParser(mainChunkFilename.toString());
            parser.parse(mainSongs, mainUsers);
            parser.close();
        }

        private void parseOtherChunk(String otherChunkFilename, Songs songs) {
            Users users = new Users();
            KDDParser parser = new KDDParser(otherChunkFilename);
            parser.parse(songs, users);
            parser.close();
        }

        @Override
        /**
         * key, otherChunk (value)
         *
         */
        public void map(Text key, Text otherChunkFilename, OutputCollector<Song, Iterator<Similarity>> output, Reporter reporter) throws IOException {
            //compare mainChunk with otherChunk
            Songs otherSongs = new Songs();
            parseOtherChunk(otherChunkFilename.toString(), otherSongs);

            //forall items i  //ith iteration
            for (Song i : mainSongs) {

                //    forall items j  //split this into N parts
                for (Song j : otherSongs) {
                    double numerator = 0, denominator_left = 0, denominator_right = 0;

                    if (j.equals(i)) {
                        continue;
                    }

                    //        forall users user
                    int userCount = 0;
                    for (User user : mainUsers) {
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
                     * sim will equal 1.0 if both songs are rated the same (have
                     * the same difference, for each user) sim will equal -1.0
                     * if the songs have the same difference but different signs
                     * NaN if no users have rated both songs If negative, should
                     * not recommend...
                     */
                    double sim = numerator / Math.sqrt(denominator_left * denominator_right);
                    if (userCount > Main.getOptions().getRatingCountThreshold()) {
                        Similarity is = new Similarity(j, sim);
                        i.getNeighborhood().insert(is);
                    }

                    //attempt to add to neighborhood (it will only be added if it should be)
                }
                output.collect(i, i.getNeighborhood().iterator());
            }
        }
    }

    /**
     * A reducer class that just emits the sum of the input values.
     */
    public static class Reduce extends MapReduceBase
            implements Reducer<Song, Similarity, Song, Iterator<Similarity>> {

        @Override
        public void reduce(Song song, Iterator<Similarity> similarities,
                OutputCollector<Song, Iterator<Similarity>> output,
                Reporter reporter) throws IOException {

            if (song.getNeighborhood() != null) {
                System.out.println("In reduce.  Expected song neighborhood to be empty, but it is not:");
                song.print();
                System.exit(1);
            }

            while (similarities.hasNext()) {
                song.addToNeighborhood(similarities.next());
            }

            output.collect(song, song.getNeighborhood().iterator());
        }
    }

    private int run(Path myChunk, FileStatus[] chunks) {
        JobConf conf = new JobConf(getConf(), ParallelKNN.class);
        conf.setJobName("KNNParallelRecommender");
        //need to add MainChunk to DistributedCache
        DistributedCache.addCacheFile(myChunk.toUri(), conf);

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(IntWritable.class);

        conf.setMapperClass(Map.class);
        conf.setCombinerClass(Reduce.class);
        conf.setReducerClass(Reduce.class);

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        for (FileStatus chunk : chunks) {
            FileInputFormat.addInputPath(conf, chunk.getPath());
        }

        String outputDir = Main.getOptions().getArgumentList().get(1);
        FileOutputFormat.setOutputPath(conf, new Path(outputDir));

        try {
            JobClient.runJob(conf);
        } catch (IOException ex) {
            Logger.getLogger(ParallelKNN.class.getName()).log(Level.SEVERE, null, ex);
        }
        return 0;
    }
}
