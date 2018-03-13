/***********************************************************************
 * Copyright (c) 2015 by Regents of the University of Minnesota.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0 which
 * accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 *
 *************************************************************************/
package edu.umn.cs.spatialHadoop.core;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

import edu.umn.cs.spatialHadoop.indexing.PartitionHTM;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.ReflectionUtils;

import edu.umn.cs.spatialHadoop.indexing.Partition;
import edu.umn.cs.spatialHadoop.mapred.GridRecordWriter2;
import edu.umn.cs.spatialHadoop.mapred.GridRecordWriter3;

/**
 * Writes a spatial file where objects are of type HTMPoint. This class is used as a
 * base class for all spatial record writers. Since a record writer must
 * implement a {@link RecordWriter} with specific keys and values, we do all
 * the implementation in this class and extend it with an implementation of
 * RecordWriter with specific key and value.
 *
 * This class is not made abstract because it is still used outside of MapReduce
 * programs.
 * @author Ahmed Eldawy
 *
 * @see edu.umn.cs.spatialHadoop.mapred.GridRecordWriter
 * @see GridRecordWriter2
 * @see GridRecordWriter3
 */
public class HTMRecordWriter {
    public static final Log LOG = LogFactory.getLog(GridRecordWriter.class);
    /**The spatial boundaries for each cell*/
    protected HTMidInfo[] htmIdInfos;

    /**Paths of intermediate files*/
    protected Path[] intermediateCellPath;

    /**An output stream for each grid cell*/
    protected OutputStream[] intermediateCellStreams;

    /**Job configuration if part of a MapReduce job*/
    protected JobConf jobConf;

    /**Path of the output directory if not part of a MapReduce job*/
    protected Path outDir;

    /**File system for output path*/
    protected final FileSystem fileSystem;

    /**Temporary text to serialize one object*/
    protected Text text;

    /**Block size for grid file written*/
    protected long blockSize;

    /**A stock object used for serialization/deserialization*/
    protected HTMPoint stockObject;

    /**An output stream to the master file*/
    protected OutputStream masterFile;

    /**A list of threads closing htmIdInfos in background*/
    protected ArrayList<Thread> closingThreads;

    /**
     * Keeps the number of elements written to each cell so far.
     * Helps calculating the overhead of RTree indexing
     */
    protected int[] intermediateCellRecordCount;

    /**Size in bytes of intermediate files written so far*/
    protected int[] intermediateCellSize;


    /**New line marker to separate records*/
    protected static byte[] NEW_LINE;

    static {
        try {
            NEW_LINE = System.getProperty("line.separator").getBytes("utf-8");
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
    }


    /**A unique prefix to all files written by this writer*/
    protected String prefix;

    /**Pack MBR of each cell around its content after it's written to disk*/
    protected boolean pack;

    /**Expand MBR of each cell to totally cover all of its contents*/
    private boolean expand;
    private int counter;

    /**Type of index being constructed*/
    private String sindex;

    /**
     * Creates a new GridRecordWriter that will write all data files to the
     * given directory
     * @param outDir The directory in which all files will be stored
     * @param job The MapReduce job associated with this output
     * @param prefix A unique prefix to be associated with files of this writer
     * @param htmIdInfos Cells to partition the file
     * @throws IOException
     */
    public HTMRecordWriter(Path outDir, JobConf job, String prefix,
                            HTMidInfo[] htmIdInfos) throws IOException {
        if (job != null) {
            this.sindex = job.get("sindex", "heap");
        }
        this.prefix = prefix;
        this.fileSystem = outDir == null ?
                FileOutputFormat.getOutputPath(job).getFileSystem(job):
                outDir.getFileSystem(job != null? job : new Configuration());
        this.outDir = outDir;
        this.jobConf = job;

        if (htmIdInfos != null) {
            // Make sure cellIndex maps to array index. This is necessary for calls that
            // call directly write(int, Text)

            // Create a master file that contains meta information about partitions
            masterFile = fileSystem.create(getMasterFilePath());

            this.htmIdInfos = htmIdInfos;

            // Prepare arrays that hold htmIdInfos information
            intermediateCellStreams = new OutputStream[this.htmIdInfos.length];
            intermediateCellPath = new Path[this.htmIdInfos.length];
            // Initialize the counters for each cell
            intermediateCellRecordCount = new int[this.htmIdInfos.length];
            intermediateCellSize = new int[this.htmIdInfos.length];

        } else {
            intermediateCellStreams = new OutputStream[1];
            intermediateCellPath = new Path[1];
            intermediateCellSize = new int[1];
            intermediateCellRecordCount = new int[1];
        }

        this.blockSize = fileSystem.getDefaultBlockSize(outDir);

        closingThreads = new ArrayList<Thread>();
        text = new Text();
    }

    protected Path getMasterFilePath() throws IOException {
        String extension = sindex;
        return getFilePath("_master."+extension);
    }

    /**
     * Returns a path to a file with the given name in the output directory
     * of the record writer.
     * @param filename
     * @return
     * @throws IOException
     */
    protected Path getFilePath(String filename) throws IOException {
        if (prefix != null)
            filename = prefix + "_" + filename;
        return outDir != null ? new Path(outDir, filename) :
                FileOutputFormat.getTaskOutputPath(jobConf, filename);
    }

    public void setStockObject(HTMPoint stockObject) {
        this.stockObject = stockObject;
    }

    public void write(int hidIndex, HTMPoint shape) throws IOException {
        writeInternal(hidIndex, shape);
    }

    /**
     * Write the given shape to the cellIndex indicated.
     * @param hidIndex
     * @param shape
     * @throws IOException
     */
    protected synchronized void writeInternal(int hidIndex, HTMPoint shape) throws IOException {
        if (hidIndex < 0) {
            // A special marker to close a cell
            closeCell(-hidIndex);
            return;
        }
        // Convert shape to text
        text.clear();
        shape.toText(text);
        // Write text representation to the file
        OutputStream cellStream = getIntermediateCellStream(hidIndex);
        cellStream.write(text.getBytes(), 0, text.getLength());
        cellStream.write(NEW_LINE);
        intermediateCellSize[hidIndex] += text.getLength() + NEW_LINE.length;
        intermediateCellRecordCount[hidIndex]++;
    }

    /**
     * Returns an output stream in which records are written as they come before
     * they are finally flushed to the cell file.
     * @param hidIndex
     * @return
     * @throws IOException
     */
    protected OutputStream getIntermediateCellStream(int hidIndex) throws IOException {
        if (intermediateCellStreams[hidIndex] == null) {
            // For grid file, we write directly to the final file
            intermediateCellPath[hidIndex] = getFinalCellPath(hidIndex);
            intermediateCellStreams[hidIndex]
                    = createFinalCellStream(intermediateCellPath[hidIndex]);
        }
        return intermediateCellStreams[hidIndex];
    }

    /**
     * Creates an output stream that will be used to write the final cell file
     * @param cellFilePath
     * @return
     * @throws IOException
     */
    protected OutputStream createFinalCellStream(Path cellFilePath)
            throws IOException {
        OutputStream cellStream;
        boolean isCompressed = jobConf != null && FileOutputFormat.getCompressOutput(jobConf);

        if (!isCompressed) {
            // Create new file
            cellStream = fileSystem.create(cellFilePath, true,
                    fileSystem.getConf().getInt("io.file.buffer.size", 4096),
                    fileSystem.getDefaultReplication(cellFilePath), this.blockSize);
        } else {
            Class<? extends CompressionCodec> codecClass =
                    FileOutputFormat.getOutputCompressorClass(jobConf, GzipCodec.class);
            // create the named codec
            CompressionCodec codec = ReflectionUtils.newInstance(codecClass, jobConf);

            // Open a stream to the output file
            cellStream = fileSystem.create(cellFilePath, true,
                    fileSystem.getConf().getInt("io.file.buffer.size", 4096),
                    fileSystem.getDefaultReplication(cellFilePath), this.blockSize);

            // Encode the output stream using the codec
            cellStream = new DataOutputStream(codec.createOutputStream(cellStream));
        }

        return cellStream;
    }

    /**
     * Closes (or initiates a close command) for the cell with the given index.
     * Once this method returns, it should be safe to reuse the same cell index
     * to write more data in a new file.
     * @param hidIndex
     * @throws IOException
     */
    protected void closeCell(int hidIndex) throws IOException {
        HTMidInfo htmIdInfo = htmIdInfos[hidIndex];

        closeCellBackground(intermediateCellPath[hidIndex],
                getFinalCellPath(hidIndex), intermediateCellStreams[hidIndex],
                masterFile, htmIdInfo, intermediateCellRecordCount[hidIndex],
                intermediateCellSize[hidIndex]);
        intermediateCellPath[hidIndex] = null;
        intermediateCellStreams[hidIndex] = null;
        intermediateCellRecordCount[hidIndex] = 0;
        intermediateCellSize[hidIndex] = 0;
    }

    /**
     * Close the given cell freeing all memory reserved by it.
     * Once a cell is closed, we should not write more data to it.
     * @param intermediateCellPath
     * @param finalCellPath
     * @param intermediateCellStream
     * @param masterFile
     * @param htmIdInfo
     * @param recordCount
     * @param cellSize
     * @throws IOException
     */
    protected void closeCellBackground(final Path intermediateCellPath,
                                       final Path finalCellPath, final OutputStream intermediateCellStream,
                                       final OutputStream masterFile, final HTMidInfo htmIdInfo,
                                       final long recordCount, final long cellSize) throws IOException {

        Thread closingThread = new Thread() {
            @Override
            public void run() {
                try {
                    Path finalfinalCellPath = flushAllEntries(intermediateCellPath,
                            intermediateCellStream, finalCellPath);
                    // Write an entry to the master file

                    // Write a line to the master file including file name and cellInfo
                    if (masterFile != null) {
                        PartitionHTM partition = new PartitionHTM(finalfinalCellPath.getName(), htmIdInfo);
                        partition.recordCount = recordCount;
                        partition.size = cellSize;
                        Text line = partition.toText(new Text());
                        masterFile.write(line.getBytes(), 0, line.getLength());
                        masterFile.write(NEW_LINE);
                    }
                } catch (IOException e) {
                    throw new RuntimeException("Error closing thread", e);
                }
            }
        };

        closingThreads.add(closingThread);
        // Remove previously terminated threads
        while (!closingThreads.isEmpty() &&
                closingThreads.get(0).getState() == Thread.State.TERMINATED) {
            closingThreads.remove(0);
        }
        // Start first thread (if exists)
        if (!closingThreads.isEmpty() && closingThreads.get(0).getState() == Thread.State.NEW)
            closingThreads.get(0).start();
    }

    /**
     * Flushes all shapes that were written to one cell to the final file.
     * It returns a path to a (closed) file that contains all entries written.
     * @param intermediateCellPath
     * @param intermediateCellStream
     * @param finalCellPath
     * @return
     * @throws IOException
     */
    protected Path flushAllEntries(Path intermediateCellPath,
                                   OutputStream intermediateCellStream, Path finalCellPath) throws IOException {
        // For global-only indexed file, the intermediate file is the final file
        intermediateCellStream.close();
        return intermediateCellPath;
    }

    /**
     * Close the whole writer. Finalize all cell files and concatenate them
     * into the output file.
     */
    public synchronized void close(Progressable progressable) throws IOException {
        // Close all output files
        for (int cellIndex = 0; cellIndex < intermediateCellStreams.length; cellIndex++) {
            if (intermediateCellStreams[cellIndex] != null) {
                closeCell(cellIndex);
            }
            // Indicate progress. Useful if closing a single cell takes a long time
            if (progressable != null)
                progressable.progress();
        }
        LOG.info("Closing record writer with "+closingThreads.size()+" remaining threads");

        while (!closingThreads.isEmpty()) {
            try {
                Thread t = closingThreads.get(0);
                switch (t.getState()) {
                    case NEW: t.start(); break;
                    case TERMINATED: closingThreads.remove(0); break;
                    default:
                        // Use limited time join to indicate progress frequently
                        t.join(10000);
                }
                // Indicate progress. Useful if closing a single cell takes a long time
                if (progressable != null)
                    progressable.progress();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        if (masterFile != null)
            masterFile.close();
    }

    /**
     * Returns path to a file in which the final cell will be written.
     * @param hidIndex The index of the cell to retrieve its output path.
     * @return
     * @throws IOException
     */
    protected Path getFinalCellPath(int hidIndex) throws IOException {
        Path path;
        do {
            String filename = counter == 0 ? String.format("data_%05d", hidIndex)
                    : String.format("data_%05d_%d", hidIndex, counter);
            boolean isCompressed = jobConf != null && FileOutputFormat.getCompressOutput(jobConf);
            if (isCompressed) {
                Class<? extends CompressionCodec> codecClass =
                        FileOutputFormat.getOutputCompressorClass(jobConf, GzipCodec.class);
                // create the named codec
                CompressionCodec codec = ReflectionUtils.newInstance(codecClass, jobConf);
                filename += codec.getDefaultExtension();
            }

            path = getFilePath(filename);
            counter++;
        } while (fileSystem.exists(path));
        return path;
    }
}
