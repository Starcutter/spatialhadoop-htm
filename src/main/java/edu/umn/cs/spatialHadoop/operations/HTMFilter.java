package edu.umn.cs.spatialHadoop.operations;

import cn.edu.tsinghua.cs.htm.utils.HTMRanges;
import cn.edu.tsinghua.cs.htm.utils.HTMid;
import cn.edu.tsinghua.cs.htm.utils.Pair;
import edu.umn.cs.spatialHadoop.core.ResultCollector;
import edu.umn.cs.spatialHadoop.indexing.PartitionHTM;

import java.util.ArrayList;
import java.util.List;

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

    public boolean overlapsWith(HTMid htmId) {
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
                    return true;
                }
            }
        } else {
            Pair<HTMid, HTMid> thatPair = htmId.extend(level);
            for (Pair<Long, Long> pair : this.pairList) {
                long lb = pair.a;
                long hb = pair.b;
                long thatLb = thatPair.a.getId();
                long thatHb = thatPair.b.getId();
                if (thatLb >= lb && thatLb <= hb ||
                        thatHb >= lb && thatHb <= hb ||
                        lb >= thatLb && lb <= thatHb ||
                        hb >= thatLb && hb <= thatHb) {
                    return true;
                }
            }
        }
        return false;
    }

    public void selectTrixels(List<PartitionHTM> pList,
                              ResultCollector<PartitionHTM> output) {
        int numPartitions = 0;
        for (PartitionHTM partition : pList) {
            if (overlapsWith(partition.htmId)) {
                output.collect(partition);
                numPartitions++;
            }
        }
        RangeQuery.LOG.info("Selected "+numPartitions+" partitions overlaps with query ranges");
    }

}
