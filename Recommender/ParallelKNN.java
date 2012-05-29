/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package Recommender;

import Database.*;
import Database.Parallel.Chunk;
import Database.Primitives.Similarity;
import Database.Primitives.Song;
import Database.Primitives.User;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;
import java.util.logging.Level;
import java.util.logging.Logger;



import org.apache.hadoop.fs.*; //Path
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*; //JobConf
import org.apache.hadoop.util.*;

/**
 *
 * @author ninj0x
 */
public class ParallelKNN extends Configured implements Recommender {

    @Override
    public void createNeighborhoods() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void recommendSong(User active, Songs songs, double threshold) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    //ChunkObject -> ChunkNeighborhoods
    public static class Map extends MapReduceBase implements Mapper<LongWritable, Chunk, Song, Iterator<Similarity>> {

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

        @Override
        /**
         * key, otherChunk (value)
         * 
         */
        public void map(LongWritable key, Chunk otherChunk, OutputCollector<Song, Iterator<Similarity>> output, Reporter reporter) throws IOException {
            //compare mainChunk with otherChunk
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

    public int run(String[] args) throws Exception {
        JobConf conf = new JobConf(getConf(), ParallelKNN.class);
        conf.setJobName("KNNParallelRecommender");

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(IntWritable.class);

        conf.setMapperClass(Map.class);
        conf.setCombinerClass(Reduce.class);
        conf.setReducerClass(Reduce.class);

        //conf.setInputFormat(TextInputFormat.class);
        //conf.setOutputFormat(TextOutputFormat.class);


        //This should be modified
        //Parse commandline input
        //need to add MainChunk to DistributedCache
        //Wherever the other files need to go to get split...
        List<String> other_args = new ArrayList<String>();
        for (int i = 0; i < args.length; ++i) {
            if ("-skip".equals(args[i])) {
                DistributedCache.addCacheFile(new Path(args[++i]).toUri(), conf);
                conf.setBoolean("wordcount.skip.patterns", true);
            } else {
                other_args.add(args[i]);
            }
        }

        FileInputFormat.setInputPaths(conf, new Path(other_args.get(0)));
        FileOutputFormat.setOutputPath(conf, new Path(other_args.get(1)));

        JobClient.runJob(conf);
        return 0;
    }
}
