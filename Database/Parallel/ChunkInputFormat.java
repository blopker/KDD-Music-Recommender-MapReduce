package Database.Parallel;
import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
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
