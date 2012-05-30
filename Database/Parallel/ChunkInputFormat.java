/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package Database.Parallel;
import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;
/**
 *
 * @author sarahejones
 */
public class ChunkInputFormat implements InputFormat<Text, Chunk> {


    @Override
  public RecordReader<Text, Chunk> getRecordReader(
      InputSplit input, JobConf job, Reporter reporter)
      throws IOException {

    reporter.setStatus(input.toString());
    return new ChunkRecordReader(job, (FileSplit)input);
  }

    @Override
    public InputSplit[] getSplits(JobConf jc, int i) throws IOException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

}
