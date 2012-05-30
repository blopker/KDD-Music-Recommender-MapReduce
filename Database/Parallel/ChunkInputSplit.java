/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package Database.Parallel;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.mapred.InputSplit;

/**
 *
 * @author sarah
 */
public class ChunkInputSplit implements InputSplit {

    @Override
    public long getLength() throws IOException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public String[] getLocations() throws IOException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void write(DataOutput d) throws IOException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void readFields(DataInput di) throws IOException {
        throw new UnsupportedOperationException("Not supported yet.");
    }
    
}
