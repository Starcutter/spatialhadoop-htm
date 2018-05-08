/***********************************************************************
 * Copyright (c) 2015 by Regents of the University of Minnesota.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0 which
 * accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 *
 *************************************************************************/
package edu.umn.cs.spatialHadoop.mapreduce;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import cn.edu.tsinghua.cs.htm.HTM;
import cn.edu.tsinghua.cs.htm.utils.HTMRanges;
import cn.edu.tsinghua.cs.htm.utils.HTMid;
import cn.edu.tsinghua.cs.htm.utils.Latlon2Cartesian;
import cn.edu.tsinghua.cs.htm.utils.Pair;
import edu.umn.cs.spatialHadoop.core.*;
import edu.umn.cs.spatialHadoop.indexing.PartitionHTM;
import edu.umn.cs.spatialHadoop.indexing.PartitionerHTM;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.io.compress.SplitCompressionInputStream;
import org.apache.hadoop.io.compress.SplittableCompressionCodec;
import org.apache.hadoop.mapred.Task;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.MapContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.LineReader;

import edu.umn.cs.spatialHadoop.OperationsParams;
import edu.umn.cs.spatialHadoop.indexing.GlobalIndex;
import edu.umn.cs.spatialHadoop.indexing.Partition;

/**
 * @author Ahmed Eldawy
 *
 */
public class HTMRecordReader<V extends Shape> extends
        RecordReader<Rectangle, Iterable<V>> {

    private static final Log LOG = LogFactory.getLog(HTMRecordReader.class);

    /**The codec used with the input file*/
    private CompressionCodec codec;
    /**The decompressor (instance) used to decompress the input file*/
    private Decompressor decompressor;

    /** File system of the file being parsed */
    private FileSystem fs;
    /**The path of the input file to read*/
    private Path path;
    /**The offset to start reading the raw (uncompressed) file*/
    private long start;
    /**The last byte to read in the raw (uncompressed) file*/
    private long end;

    /**
     * The input stream that reads directly from the input file.
     * If the file is not compressed, this stream is the same as #in.
     * Otherwise, this is the raw (compressed) input stream. This stream is used
     * only to calculate the progress of the input file.
     */
    private FSDataInputStream directIn;
    /** Input stream that reads data from input file */
    private InputStream in;
    /**Determine current position to report progress*/
    private Seekable progressPosition;

    /**Used to read text lines from the input*/
    private LineReader lineReader;

    /**The shape used to parse input lines*/
    private V stockShape;

    private HTMid[] childHTMid;

    private int numChild;

    private long[] childOffset;

    private int nextChild;

    private Text tempLine;

    private HTMRanges inputQueryRange;

    private List<Pair<Long, Long> > pairList;

    private CompressionCodecFactory compressionCodecFactory;

    private ShapeIterator<V> value;

    /**
     * Number of bytes read from the input so far. This is used to determine when
     * to stop when reading from the input directly. We canno simply rely on the
     * position of the input file because LineReader might buffer some data in
     * memory without actually processing it.
     */
    private long bytesRead;

    /**
     * Context of the map task. Used to increment counters.
     */
    private Counter inputRecordsCounter;

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context)
            throws IOException, InterruptedException {
        Configuration conf = context != null ? context.getConfiguration() : new Configuration();
        if (context != null && context instanceof MapContext)
            inputRecordsCounter = (context).getCounter(Task.Counter.MAP_INPUT_RECORDS);
        initialize(split, conf);
    }

    public void initialize(InputSplit split, Configuration conf)
            throws IOException, InterruptedException {
        FileSplit fsplit = (FileSplit) split;
        if (compressionCodecFactory == null)
            compressionCodecFactory = new CompressionCodecFactory(conf);

        LOG.info("Open a SpatialRecordReader to split: "+split);
        this.path = fsplit.getPath();
        this.start = fsplit.getStart();
        this.end = this.start + split.getLength();
        this.fs = this.path.getFileSystem(conf);
        this.directIn = fs.open(this.path);
        codec = compressionCodecFactory.getCodec(this.path);

        if (codec != null) {
            // Input is compressed, create a decompressor to decompress it
            decompressor = CodecPool.getDecompressor(codec);
            if (codec instanceof SplittableCompressionCodec) {
                // A splittable compression codec, can seek to the desired input pos
                final SplitCompressionInputStream cIn =
                        ((SplittableCompressionCodec)codec).createInputStream(
                                directIn, decompressor, start, end,
                                SplittableCompressionCodec.READ_MODE.BYBLOCK);
                in = cIn;
                start = cIn.getAdjustedStart();
                end = cIn.getAdjustedEnd();
                // take pos from compressed stream as we adjusted both start and end
                // to match with the compressed file
                progressPosition = cIn;
            } else {
                // Non-splittable input, need to start from the beginning
                CompressionInputStream cIn = codec.createInputStream(directIn, decompressor);
                in = cIn;
                progressPosition = cIn;
            }
        } else {
            // Non-compressed file, seek to the desired position and use this stream
            // to get the progress and position
            directIn.seek(start);
            in = directIn;
            progressPosition = directIn;
        }
        this.stockShape = (V) OperationsParams.getShape(conf, "shape");

        this.tempLine = new Text();
        this.lineReader = new LineReader(in);
        bytesRead = 0;

        if (this.start != 0) {
            // Skip until first end-of-line reached
            bytesRead += lineReader.readLine(tempLine);
        }

        if (!(this.stockShape instanceof HTMPoint)) {
            DataInput din = new DataInputStream(in);
            this.numChild = din.readInt();
            this.childHTMid = new HTMid[this.numChild];
            bytesRead += 4;
            this.childOffset = new long[this.numChild];
            for (int i = 0; i < this.numChild; i++) {
                this.childHTMid[i] = new HTMid(din.readLong());
                this.childOffset[i] = din.readLong();
                bytesRead += 16;
            }
            this.nextChild = 0;
        }

        if (conf.get("ranges") != null) {
            // Retrieve the input query range to apply on all records
            //this.inputQueryRange = OperationsParams.getRanges(conf, "ranges");
            this.inputQueryRange = OperationsParams.getRanges(conf, this.path.getName());
        }

        this.value = new ShapeIterator<V>();
        value.setShape(stockShape);
    }

    public long getPos() throws IOException {
        if (codec != null) {
            // Input is compressed. Report the progress as indicated by the
            // decompressor
            return progressPosition.getPos();
        } else {
            // Input is not compressed. Report the progress as indicated by number
            // of bytes read from the input
            return start + bytesRead;
        }
    }

    /**
     * Reads the next line from input and return true if a line was read.
     * If no more lines are available in this split, a false is returned.
     * @param value The text object to fill with the next line
     * @return <code>true</code> if a line was read; <code>false</code> otherwise.
     * @throws IOException If an error occurs while reading from disk.
     */
    protected boolean nextLine(Text value) throws IOException {
        while (getPos() <= end) {
            value.clear();

            int lineLength;
            // Read the first line from stream
            if ((lineLength = lineReader.readLine(value)) <= 0) {
                // Indicates an end of stream
                return false;
            }

            // Append the part read from stream to the part extracted from buffer
            bytesRead += lineLength;

            if (value.getLength() > 1) {
                // Read a non-empty line. Note that end-of-line character is included
                return true;
            }
        }
        // Reached end of file
        return false;
    }

    protected boolean isMatched(Shape shape) {
        // Match with the query
        if (inputQueryRange != null && (shape == null ))
            return false;
        if (inputQueryRange == null)
            return true;
        long id = 0;
        if (shape instanceof HTMPoint) {
            id = ((HTMPoint) shape).HTMid;
        } else {
            Point p = ((Point) shape);
            id = HTM.getInstance().Cartesian2HTMid(
                    Latlon2Cartesian.parse(p.x, p.y), PartitionerHTM.pointLevel
            ).getId();
        }
        if (this.pairList == null) {
            extendRanges(new HTMid(id).getLevel());
        }
        for (Pair<Long, Long> pair : this.pairList) {
            if (pair.a <= id && id <= pair.b) {
                return true;
            }
        }
        return false;
    }

    protected void extendRanges(int level) {
        this.pairList = new ArrayList<Pair<Long, Long>>();
        List<Pair<HTMid, HTMid> > pairList = inputQueryRange.getPairList();
        for (Pair<HTMid, HTMid> pair : pairList) {
            long lb = pair.a.extend(level).a.getId();
            long hb = pair.b.extend(level).b.getId();
            this.pairList.add(new Pair<Long, Long>(lb, hb));
        }
    }

    protected boolean queryIntersectsWithChild(int childIndex) {
        HTMid childId = this.childHTMid[childIndex];
        List<Pair<HTMid, HTMid> > pairList = inputQueryRange.getPairList();
        int pairLevel = pairList.get(0).a.getLevel();
        if (childId.getLevel() < pairLevel) {
            Pair<HTMid, HTMid> childPair = childId.extend(pairLevel);
            long childLb = childPair.a.getId();
            long childHb = childPair.b.getId();
            for (Pair<HTMid, HTMid> pair : pairList) {
                long lb = pair.a.getId();
                long hb = pair.b.getId();
                if (childLb >= lb && childLb <= hb ||
                        childHb >= lb && childHb <= hb ||
                        lb >= childLb && lb <= childHb ||
                        hb >= childLb && hb <= childHb) {
                    return true;
                }
            }
            return false;
        } else {
            return this.inputQueryRange.contains(childId);
        }
    }

    /**
     * Reads next shape from input and returns true. If no more shapes are left
     * in the split, a false is returned. This function first reads a line
     * by calling the method {@link #nextLine(Text)} then parses the returned
     * line by calling {@link Shape#fromText(Text)} on that line. If no stock
     * shape is set, a {@link NullPointerException} is thrown.
     * @param s A mutable shape object to update with the next value
     * @return <code>true</code> if an object was read; <code>false</code> if end-of-file was reached.
     * @throws IOException If an error happens while reading from disk
     */
    protected boolean nextShape(V s) throws IOException {
        if (this.stockShape instanceof HTMPoint) {
            do {
                if (!nextLine(tempLine))
                    return false;
                s.fromText(tempLine);
            } while (!isMatched(s));
            return true;
        } else {

            if (this.nextChild < this.numChild &&
                    start + bytesRead >= this.childOffset[this.nextChild]) {
                while (!queryIntersectsWithChild(this.nextChild)) {
                    if (this.nextChild == this.numChild - 1) {
                        return false;
                    }
                    this.nextChild++;
                    bytesRead = this.childOffset[this.nextChild] - start;
                    directIn.seek(this.childOffset[this.nextChild]);
                    in = directIn;
                }
                this.nextChild++;
                lineReader = new LineReader(in);
            }
            long nextOffset = this.nextChild < this.numChild ?
                    this.childOffset[this.nextChild] :
                    this.end;
            boolean matched = false;
            while (start + bytesRead < nextOffset) {
                if (!nextLine(tempLine)) {
                    return false;
                }
                //LOG.info(tempLine);
                s.fromText(tempLine);
                if (isMatched(s)) {
                    matched = true;
                    break;
                }
            }
            return matched;

        }
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        value.setSpatialRecordReader(this);
        return value.hasNext();
    }

    @Override
    public Rectangle getCurrentKey() throws IOException, InterruptedException {
        return new Rectangle();
    }

    @Override
    public Iterable<V> getCurrentValue() throws IOException,
            InterruptedException {
        return value;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        if (start == end) {
            return 0.0f;
        } else {
            return Math.min(1.0f,
                    (directIn.getPos() - start) / (float)(end - start));
        }
    }

    @Override
    public void close() throws IOException {
        try {
            if (lineReader != null) {
                lineReader.close();
            } else if (in != null) {
                in.close();
            }
            lineReader = null;
            in = null;
        } finally {
            if (decompressor != null) {
                CodecPool.returnDecompressor(decompressor);
            }
        }
    }

    /**
     * An iterator that iterates over all shapes in the input file
     * @author Eldawy
     */
    public static class ShapeIterator<V extends Shape>
            implements Iterator<V>, Iterable<V> {
        protected V shape;
        protected V nextShape;
        private HTMRecordReader<V> srr;

        public ShapeIterator() {
        }

        public void setSpatialRecordReader(HTMRecordReader<V> srr) {
            this.srr = srr;
            try {
                if (shape != null)
                    nextShape = (V) shape.clone();
                if (nextShape != null && !srr.nextShape(nextShape))
                    nextShape = null;
            } catch (IOException e) {
                throw new RuntimeException("Error reading from file", e);
            }
        }

        public void setShape(V shape) {
            this.shape = shape;
            this.nextShape = (V) shape.clone();
            try {
                if (srr != null && !srr.nextShape(nextShape))
                    nextShape = null;
            } catch (IOException e) {
                throw new RuntimeException("Error eading from file", e);
            }
        }

        public boolean hasNext() {
            return nextShape != null;
        }

        @Override
        public V next() {
            try {
                if (nextShape == null)
                    return null;
                // Swap Shape and nextShape and read next
                V temp = shape;
                shape = nextShape;
                nextShape = temp;

                if (!srr.nextShape(nextShape))
                    nextShape = null;
                if (srr.inputRecordsCounter != null)
                    srr.inputRecordsCounter.increment(1);
                return shape;
            } catch (IOException e) {
                throw new RuntimeException("Error reading from file", e);
            }
        }

        @Override
        public Iterator<V> iterator() {
            return this;
        }

        @Override
        public void remove() {
            throw new RuntimeException("Unsupported method ShapeIterator#remove");
        }

    }

}
