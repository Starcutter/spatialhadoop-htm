package edu.umn.cs.spatialHadoop.indexing;

import edu.umn.cs.spatialHadoop.core.Shape;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.io.IOException;

public class HTMLocalIndexer implements LocalIndexer {
    @Override
    public void setup(Configuration conf) throws IOException, InterruptedException {

    }

    @Override
    public String getExtension() {
        return null;
    }

    @Override
    public void buildLocalIndex(File nonIndexedFile, Path outputIndexedFile, Shape shape) throws IOException, InterruptedException {

    }
}
