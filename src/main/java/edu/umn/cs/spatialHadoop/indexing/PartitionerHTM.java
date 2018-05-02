package edu.umn.cs.spatialHadoop.indexing;

import cn.edu.tsinghua.cs.htm.HTM;
import cn.edu.tsinghua.cs.htm.utils.HTMid;
import cn.edu.tsinghua.cs.htm.utils.Latlon2Cartesian;
import edu.umn.cs.spatialHadoop.core.HTMPoint;
import edu.umn.cs.spatialHadoop.core.HTMidInfo;
import edu.umn.cs.spatialHadoop.core.Point;
import edu.umn.cs.spatialHadoop.core.Shape;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.mortbay.log.Log;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

public class PartitionerHTM implements Writable {
    private static final org.apache.commons.logging.Log LOG = LogFactory.getLog(PartitionerHTM.class);
    private static final String PartitionerClass = "Partitioner.Class";
    private static final String PartitionerValue = "Partitioner.Value";

    public static final int pointLevel = 10;
    private HTM htm = HTM.getInstance();
    private HTMidInfo[] htmIdInfos;

    public void createFromPoints(HTMidInfo[] htmIdInfos, Point[] points,
                                 long estimatedOutSize, long outBlockSize) {

        int initLen = htmIdInfos.length;
        int averLevel = htmIdInfos[0].htmId.getLevel();
        int beginIndex = 8 * ((1 << (averLevel << 1)) - 1) / 3;
        int treeSize = beginIndex + initLen;

        long numPoints[] = new long[treeSize];
        List<List<Integer>> insidePointIndexes = new ArrayList<List<Integer>>();
        long ids[] = new long[treeSize];
        for (int j = beginIndex; j < treeSize; j++) {
            ids[j] = htmIdInfos[j - beginIndex].htmId.getId();
            insidePointIndexes.add(new ArrayList<Integer>());
        }
        boolean exist[] = new boolean[treeSize];
        long numPointsThreshold = points.length * outBlockSize / estimatedOutSize;

        // calculate HTMids of points and build leaves of quad tree
        long[] pointsId = new long[points.length];
        for (int i = 0; i < points.length; i++) {
            Point p = points[i];
            HTMid hid = htm.Cartesian2HTMid(Latlon2Cartesian.parse(p.x, p.y), pointLevel);
            pointsId[i] = hid.getId();
            for (int j = beginIndex; j < treeSize; j++) {
                long truncated = hid.getId() >> (2 * (pointLevel - averLevel));
                if (ids[j] == truncated) {
                    numPoints[j] += 1L;
                    insidePointIndexes.get(j - beginIndex).add(i);
                    exist[j] = true;
                }
            }
        }

        // process the tree leaves
        LinkedList<HTMidInfo> finalInfos = new LinkedList<HTMidInfo>();
        Queue<Long> needSplit = new LinkedList<Long>();
        Queue<List<Integer>> pointIndexesQueue = new LinkedList<List<Integer>>();
        for (int i = beginIndex; i < treeSize; i += 4) {
            int cntExist = 0;
            long cntPointsSum = 0L;
            boolean exceed[] = new boolean[]{false, false, false, false};
            for (int j = i; j < i + 4; j++) {
                if (exist[j]) {
                    cntExist++;
                    cntPointsSum += numPoints[j];
                    if (numPoints[j] > numPointsThreshold) {
                        exceed[j - i] = true;
                        needSplit.add(ids[j]);
                        pointIndexesQueue.add(insidePointIndexes.get(j - beginIndex));
                    }
                }
            }
            if (averLevel > 0 && cntExist == 4 && cntPointsSum < numPointsThreshold) {
                int parent = (i >> 2) - 2;
                numPoints[parent] = cntPointsSum;
                exist[parent] = true;
                ids[parent] = ids[i] >> 2;
            } else {
                for (int j = i; j < i + 4; j++) {
                    if (exist[j] && !exceed[j - i]) {
                        finalInfos.add(htmIdInfos[j - beginIndex]);
                    }
                }
            }
        }

        // merge upward
        while (beginIndex > 0) {
            treeSize = beginIndex;
            beginIndex = (beginIndex >> 2) - 2;
            for (int i = beginIndex; i < treeSize; i += 4) {
                int cntExist = 0;
                long cntPointsSum = 0L;
                for (int j = i; j < i + 4; j++) {
                    if (exist[j]) {
                        cntExist++;
                        cntPointsSum += numPoints[j];
                    }
                }
                if (beginIndex > 0 && cntExist == 4 && cntPointsSum < numPointsThreshold) {
                    int parent = (i >> 2) - 2;
                    numPoints[parent] = cntPointsSum;
                    exist[parent] = true;
                    ids[parent] = ids[i] >> 2;
                } else {
                    for (int j = i; j < i + 4; j++) {
                        if (exist[j]) {
                            finalInfos.addFirst(new HTMidInfo(ids[j]));
                        }
                    }
                }
            }
        }

        // split downward
        int currentLevel = averLevel;
        while (!needSplit.isEmpty() && currentLevel < pointLevel) {
            long hid = needSplit.peek();
            needSplit.remove();
            List<Integer> parentPointIndexes = pointIndexesQueue.peek();
            pointIndexesQueue.remove();
            long children[] = new long[]{(hid << 2), (hid << 2) + 1, (hid << 2) + 2, (hid << 2) + 3};
            List<List<Integer>> childPointIndexesList = new ArrayList<List<Integer>>();
            for (int j = 0; j < 4; j++) {
                childPointIndexesList.add(new LinkedList<Integer>());
            }
            for (int i = 0; i < parentPointIndexes.size(); i++) {
                long truncated = pointsId[i] >> (2 * (pointLevel - currentLevel));
                for (int j = 0; j < 4; j++) {
                    if (children[j] == truncated) {
                        childPointIndexesList.get(j).add(i);
                    }
                }
            }
            for (int j = 0; j < 4; j++) {
                List<Integer> childPointIndexes = childPointIndexesList.get(j);
                if (childPointIndexes.size() > numPointsThreshold) {
                    needSplit.add(children[j]);
                    pointIndexesQueue.add(childPointIndexes);
                } else {
                    finalInfos.add(new HTMidInfo(children[j]));
                }
            }
            currentLevel++;
        }

        for (long hid : needSplit) {
            finalInfos.add(new HTMidInfo(hid));
        }

        this.htmIdInfos = finalInfos.toArray(new HTMidInfo[finalInfos.size()]);
        LOG.info("Partitioned data into " + this.htmIdInfos.length + " partitions.");
    }

    public int overlapPartition(Shape shape) {
        Point p = (Point) shape;
        int i = 0;
        long pointId = htm.Cartesian2HTMid(Latlon2Cartesian.parse(p.x, p.y), pointLevel).getId();
        for (HTMidInfo hidInfo : this.htmIdInfos) {
            HTMid partitionId = hidInfo.htmId;
            long truncated = pointId >> (2 * (pointLevel - partitionId.getLevel()));
            if (truncated == partitionId.getId()) {
                return i;
            }
            i++;
        }
        return -1;
    }

    public static void setPartitioner(Configuration conf, PartitionerHTM partitioner) throws IOException {
        conf.setClass(PartitionerClass, partitioner.getClass(), PartitionerHTM.class);
        Path tempFile;
        FileSystem fs = FileSystem.get(conf);
        do {
            tempFile = new Path("cells_"+(int)(Math.random()*1000000)+".partitions");
        } while (fs.exists(tempFile));
        FSDataOutputStream out = fs.create(tempFile);
        partitioner.write(out);
        out.close();

        fs.deleteOnExit(tempFile);

        DistributedCache.addCacheFile(tempFile.toUri(), conf);
        conf.set(PartitionerValue, tempFile.getName());
    }

    /**
     * Retrieves the value of a partitioner for a given job.
     * @param conf
     * @return
     */
    public static PartitionerHTM getPartitioner(Configuration conf) {
        Class<? extends PartitionerHTM> klass =
                conf.getClass(PartitionerClass, PartitionerHTM.class).asSubclass(PartitionerHTM.class);
        if (klass == null)
            return null;
        try {
            PartitionerHTM partitioner = klass.newInstance();

            String partitionerFile = conf.get(PartitionerValue);
            if (partitionerFile != null) {
                Path[] cacheFiles = DistributedCache.getLocalCacheFiles(conf);
                for (Path cacheFile : cacheFiles) {
                    if (cacheFile.getName().contains(partitionerFile)) {
                        FSDataInputStream in = FileSystem.getLocal(conf).open(cacheFile);
                        partitioner.readFields(in);
                        in.close();
                    }
                }
            }
            return partitioner;
        } catch (InstantiationException e) {
            Log.warn("Error instantiating partitioner", e);
            return null;
        } catch (IllegalAccessException e) {
            Log.warn("Error instantiating partitioner", e);
            return null;
        } catch (IOException e) {
            Log.warn("Error retrieving partitioner value", e);
            return null;
        }
    }

    public int getPartitionCount() {
        return this.htmIdInfos.length;
    }

    public HTMidInfo getHTMidInfo(int id) {
        return htmIdInfos[id];
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(this.htmIdInfos.length);
        for (HTMidInfo htmIdInfo : this.htmIdInfos) {
            out.writeLong(htmIdInfo.htmId.getId());
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        int len = in.readInt();
        this.htmIdInfos = new HTMidInfo[len];
        for (int i = 0; i < len; i++) {
            long hid = in.readLong();
            this.htmIdInfos[i] = new HTMidInfo(hid);
        }
    }
}
