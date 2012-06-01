/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package Recommender;

import Database.KDDParser;
import Database.Parallel.Chunk;
import Database.Primitives.Similarity;
import Database.Primitives.Song;
import Database.Primitives.User;
import Database.Songs;
import Database.Users;
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
import org.apache.hadoop.io.LongWritable;
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
            Path chunkDir = new Path(Main.Main.getOptions().getArgumentList().get(0));
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

        //conf.setInputFormat(TextInputFormat.class);
        //conf.setOutputFormat(TextOutputFormat.class);
        
        for(FileStatus chunk: chunks){
            FileInputFormat.addInputPath(conf, chunk.getPath());
        }   
        
        String outputDir = Main.Main.getOptions().getArgumentList().get(1);
        FileOutputFormat.setOutputPath(conf, new Path(outputDir));
        
        try {
            JobClient.runJob(conf);
        } catch (IOException ex) {
            Logger.getLogger(ParallelKNN.class.getName()).log(Level.SEVERE, null, ex);
        }
        return 0;
    }
}
