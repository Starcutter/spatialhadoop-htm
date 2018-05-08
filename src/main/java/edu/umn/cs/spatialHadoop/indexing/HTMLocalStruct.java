package edu.umn.cs.spatialHadoop.indexing;

import cn.edu.tsinghua.cs.htm.HTM;
import cn.edu.tsinghua.cs.htm.utils.HTMid;
import cn.edu.tsinghua.cs.htm.utils.Latlon2Cartesian;
import edu.umn.cs.spatialHadoop.core.Point;
import edu.umn.cs.spatialHadoop.core.Shape;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;

import java.io.DataOutput;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;

public class HTMLocalStruct {

    private static final Log LOG = LogFactory.getLog(HTMLocalStruct.class);

    /**New line marker to separate records*/
    protected static byte[] NEW_LINE;

    static {
        try {
            NEW_LINE = System.getProperty("line.separator", "\n").getBytes("utf-8");
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
            throw new RuntimeException("Cannot retrieve system line separator", e);
        }
    }

    public static final int localDepth = 2;

    public static int skipToEOL(byte[] bytes, int startOffset) {
        int eol = startOffset;
        while (eol < bytes.length && (bytes[eol] != '\n' && bytes[eol] != '\r'))
            eol++;
        while (eol < bytes.length && (bytes[eol] == '\n' || bytes[eol] == '\r'))
            eol++;
        return eol;
    }

    public static void bulkLoadWrite(final byte[] element_bytes,
                                     final int offset, final int len,
                                     long parentHid,
                                     DataOutput dataOut, final Shape stockObject) throws IOException {
        int numChild = 1 << (2 * localDepth);
        HTMid childHTMids[] = new HTMid[numChild];
        List<List<Integer>> childStartPosList = new ArrayList<List<Integer>>();
        List<List<Integer>> childLineLenList = new ArrayList<List<Integer>>();
        for (int i = 0; i < numChild; i++) {
            childHTMids[i] = new HTMid((parentHid << (2 * localDepth)) + i);
            childStartPosList.add(new ArrayList<Integer>());
            childLineLenList.add(new ArrayList<Integer>());
        }
        int pointLevel = PartitionerHTM.pointLevel;
        HTM htm = HTM.getInstance();

        int i_start = offset;
        final Text line = new Text();
        while (i_start < offset + len) {
            int i_end = skipToEOL(element_bytes, i_start);
            // Extract the line without end of line character
            line.set(element_bytes, i_start, i_end - i_start - 1);
            stockObject.fromText(line);
            Point p = (Point) stockObject;
            long pointId = htm.Cartesian2HTMid(Latlon2Cartesian.parse(p.x, p.y), pointLevel).getId();
            for (int i = 0; i < numChild; i++) {
                HTMid childHTMid = childHTMids[i];
                long truncated = pointId >> (2 * (pointLevel - childHTMid.getLevel()));
                if (truncated == childHTMid.getId()) {
                    childStartPosList.get(i).add(i_start);
                    childLineLenList.get(i).add(i_end - i_start - 1);
                }
            }
            i_start = i_end;
        }

        List<Long> childLenSums = new ArrayList<Long>();
        for (int i = 0; i < numChild; i++) {
            List<Integer> childLineLen = childLineLenList.get(i);
            long sum = 0L;
            for (int l : childLineLen) {
                sum += l + NEW_LINE.length;
            }
            childLenSums.add(sum);
        }


        dataOut.writeInt(numChild);
        long bytesCnt = 0L;
        bytesCnt += 4 + 16 * numChild;
        for (int i = 0; i < numChild; i++) {
            //int pointNum = childStartPosList.get(i).size();
            //dataOut.writeInt(pointNum);
            dataOut.writeLong(childHTMids[i].getId());
            dataOut.writeLong(bytesCnt);
            bytesCnt += childLenSums.get(i);
        }
        for (int i = 0; i < numChild; i++) {
            List<Integer> childStartPos = childStartPosList.get(i);
            List<Integer> childLineLen = childLineLenList.get(i);
            for (int j = 0; j < childStartPos.size(); j++) {
                int startPos = childStartPos.get(j);
                int lineLen = childLineLen.get(j);
                dataOut.write(element_bytes, startPos, lineLen);
                dataOut.write(NEW_LINE);
            }
        }
    }
}
