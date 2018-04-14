package edu.umn.cs.spatialHadoop.operations;

import cn.edu.tsinghua.cs.htm.utils.HTMRanges;
import cn.edu.tsinghua.cs.htm.utils.HTMid;
import cn.edu.tsinghua.cs.htm.utils.Pair;
import edu.umn.cs.spatialHadoop.core.ResultCollector;
import edu.umn.cs.spatialHadoop.indexing.PartitionHTM;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HTMFilter {
    public HTMRanges ranges;
    public List<Pair<Long, Long> > pairList;
    public int level;

    public HTMFilter(HTMRanges ranges) {
        this.ranges = ranges;
        level = ranges.getPairList().get(0).a.getLevel();
    }

    public void extendRanges(int thatLevel) {
        this.pairList = new ArrayList<Pair<Long, Long>>();
        List<Pair<HTMid, HTMid> > pairList = this.ranges.getPairList();
        if (level <= thatLevel) {
            for (Pair<HTMid, HTMid> pair : pairList) {
                long lb = pair.a.extend(thatLevel).a.getId();
                long hb = pair.b.extend(thatLevel).b.getId();
                this.pairList.add(new Pair<Long, Long>(lb, hb));
            }
        } else {
            for (Pair<HTMid, HTMid> pair : pairList) {
                this.pairList.add(new Pair<Long, Long>(pair.a.getId(), pair.b.getId()));
            }
        }
    }

    public boolean overlapsWith(PartitionHTM partition, Map<String, String> filename2Ranges) {
        HTMid htmId = partition.htmId;
        String filename = partition.filename;
        int thatLevel = htmId.getLevel();
        long hid = htmId.getId();

        if (this.pairList == null) {
            extendRanges(thatLevel);
        }

        if (level <= thatLevel) {
            for (Pair<Long, Long> pair : this.pairList) {
                long lb = pair.a;
                long hb = pair.b;
                if (lb <= hid && hid <= hb) {
                    filename2Ranges.put(filename, String.valueOf(lb) + ',' + String.valueOf(hb));
                    return true;
                }
            }
        } else {
            Pair<HTMid, HTMid> thatPair = htmId.extend(level);
            boolean overlap = false;
            StringBuilder rangesStr = new StringBuilder();
            for (Pair<Long, Long> pair : this.pairList) {
                long lb = pair.a;
                long hb = pair.b;
                long thatLb = thatPair.a.getId();
                long thatHb = thatPair.b.getId();
                if (thatLb >= lb && thatLb <= hb ||
                        thatHb >= lb && thatHb <= hb ||
                        lb >= thatLb && lb <= thatHb ||
                        hb >= thatLb && hb <= thatHb) {
                    overlap = true;
                    if (rangesStr.length() == 0) {
                        rangesStr.append(lb);
                        rangesStr.append(',');
                        rangesStr.append(hb);
                    } else {
                        rangesStr.append(';');
                        rangesStr.append(lb);
                        rangesStr.append(',');
                        rangesStr.append(hb);
                    }
                }
            }
            filename2Ranges.put(filename, rangesStr.toString());
            return overlap;
        }
        return false;
    }

    public void selectTrixels(List<PartitionHTM> pList,
                              Map<String, String> filename2Ranges,
                              ResultCollector<PartitionHTM> output) {
        int numPartitions = 0;
        for (PartitionHTM partition : pList) {
            if (overlapsWith(partition, filename2Ranges)) {
                output.collect(partition);
                numPartitions++;
            }
        }
        RangeQuery.LOG.info("Selected "+numPartitions+" partitions overlaps with query ranges");
    }

}
