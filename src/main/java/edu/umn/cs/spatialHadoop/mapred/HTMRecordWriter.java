package edu.umn.cs.spatialHadoop.mapred;

import edu.umn.cs.spatialHadoop.core.HTMPoint;
import edu.umn.cs.spatialHadoop.core.HTMidInfo;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;

public class HTMRecordWriter
        extends edu.umn.cs.spatialHadoop.core.HTMRecordWriter
        implements RecordWriter<IntWritable, HTMPoint> {

    public HTMRecordWriter(JobConf job, String name, HTMidInfo[] htmIdInfos) throws IOException {
        super(null, job, name, htmIdInfos);
    }

    @Override
    public void write(IntWritable key, HTMPoint value) throws IOException {
        super.write(key.get(), value);
    }

    @Override
    public void close(Reporter reporter) throws IOException {
        super.close(reporter);
    }
}