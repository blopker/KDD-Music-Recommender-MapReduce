/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package Main;

import Database.Songs;
import Database.Users;
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
 * @author sarah
 */
public class ParallelRecommender extends Configured {
    
    //ChunkObject -> ChunkNeighborhoods
    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {

    private Songs mainSongs;    //my chunk Songs
    private Users mainUsers;    //my chunk Users  
        
        @Override
    public void configure(JobConf job) {
        //get the mainChunk (from cache hopefully)
        Path[] localFiles = new Path[0];
        try {
            localFiles = DistributedCache.getLocalCacheFiles(job);
        } catch (IOException ioe) {
            System.err.println("Caught exception while getting cached files: " + StringUtils.stringifyException(ioe));
        }
        parseMainChunk(localFiles[0]);
    }    

    private void parseMainChunk(Path mainChunkFilename) {
        try {
            BufferedReader mainChunk = new BufferedReader(new FileReader(
                    mainChunkFilename.toString()));
            String pattern = null;
            //parse mainChunk here
                //fill mainSongs
                //fill mainUsers
        } catch (IOException ioe) {
            System.err.println("Caught exception while parsing the cached file '"
                    + mainChunkFilename + "' : " + StringUtils.stringifyException(ioe));
        }
    }
    
    public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
     
    }
  }

    /**
    * A reducer class that just emits the sum of the input values.
    */
    public static class Reduce extends MapReduceBase
        implements Reducer<Text, IntWritable, Text, IntWritable> {

        @Override
        public void reduce(Text key, Iterator<IntWritable> values,
                        OutputCollector<Text, IntWritable> output,
                        Reporter reporter) throws IOException {
        int sum = 0;
        while (values.hasNext()) {
            sum += values.next().get();
        }
        output.collect(key, new IntWritable(sum));
        }
    }
  
    public int run(String[] args) throws Exception {
        JobConf conf = new JobConf(getConf(), ParallelRecommender.class);
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
        for (int i=0; i < args.length; ++i) {
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
