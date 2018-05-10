package edu.umn.cs.spatialHadoop.indexing;

import edu.umn.cs.spatialHadoop.core.Shape;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class HTMLocalIndexer implements LocalIndexer {
    private static final Log LOG = LogFactory.getLog(HTMLocalIndexer.class);

    /**Configuration of the running job*/
    protected Configuration conf;

    @Override
    public void setup(Configuration conf) throws IOException, InterruptedException {
        this.conf = conf;
    }

    @Override
    public String getExtension() {
        return "htm";
    }

    @Override
    public void buildLocalIndex(File nonIndexedFile, Path outputIndexedFile,
                                Shape shape) throws IOException, InterruptedException {
        // Read all data of the written file in memory
        byte[] cellData = new byte[(int) nonIndexedFile.length()];
        InputStream cellIn = new BufferedInputStream(new FileInputStream(nonIndexedFile));
        cellIn.read(cellData);
        cellIn.close();

        String filename = nonIndexedFile.getName();
        Pattern p = Pattern.compile("part-(\\d+)-");
        Matcher m = p.matcher(filename);
        if (m.find()) {
            long hid = Long.parseLong(m.group(1));

            // Create the output file
            FileSystem outFS = outputIndexedFile.getFileSystem(conf);
            DataOutputStream cellStream = outFS.create(outputIndexedFile);
            HTMLocalStruct.bulkLoadWrite(cellData, 0, cellData.length,
                    hid, cellStream, shape.clone());
            cellStream.close();
        }
    }
}
