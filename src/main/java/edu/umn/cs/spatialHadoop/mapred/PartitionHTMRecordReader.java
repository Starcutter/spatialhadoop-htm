package edu.umn.cs.spatialHadoop.mapred;

import edu.umn.cs.spatialHadoop.indexing.PartitionHTM;
import org.apache.hadoop.io.NullWritable;

import java.io.IOException;
import java.io.InputStream;

public class PartitionHTMRecordReader
        extends SpatialRecordReader<NullWritable, PartitionHTM> {

    public PartitionHTMRecordReader(InputStream is, long offset, long endOffset)
            throws IOException {
        super(is, offset, endOffset);
    }

    @Override
    public boolean next(NullWritable key, PartitionHTM value) throws IOException {
        if (!nextLine(tempLine)) {
            return false;
        }
        value.fromText(tempLine);
        return true;
    }

    @Override
    public NullWritable createKey() {
        return NullWritable.get();
    }

    @Override
    public PartitionHTM createValue() {
        return new PartitionHTM();
    }
}
