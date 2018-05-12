package edu.umn.cs.spatialHadoop.indexing;

import cn.edu.tsinghua.cs.htm.HTM;
import cn.edu.tsinghua.cs.htm.utils.HTMid;
import cn.edu.tsinghua.cs.htm.utils.Latlon2Cartesian;
import com.sun.org.apache.bcel.internal.generic.NEW;
import edu.umn.cs.spatialHadoop.core.HTMPoint;
import edu.umn.cs.spatialHadoop.core.Point;
import edu.umn.cs.spatialHadoop.core.Shape;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;

import java.io.DataOutput;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
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

    public static final int localDepth = 1;

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
        long childHids[] = new long[numChild];
        int childDepths[] = new int[numChild];
        for (int i = 0; i < numChild; i++) {
            HTMid childHTMid = new HTMid((parentHid << (2 * localDepth)) + i);
            childHids[i] = childHTMid.getId();
            childDepths[i] = childHTMid.getLevel();
        }

        int childLen[] = new int[numChild];
        for (int i = 0; i < numChild; i++) {
            childLen[i] = 0;
        }
        int recordCnt = 0;

        int i_start = offset;
        while (i_start < offset + len) {
            i_start = skipToEOL(element_bytes, i_start);
            recordCnt++;
        }

        long recordHid[] = new long[recordCnt];

        int pointLevel = PartitionerHTM.pointLevel;

        int cnt = 0;
        i_start = offset;
        final Text line = new Text();
        while (i_start < offset + len) {
            int i_end = skipToEOL(element_bytes, i_start);
            // Extract the line without end of line character
            line.set(element_bytes, i_start, i_end - i_start - 1);
            String s = line.toString();
            int idLen = s.split(",")[0].length() + 1;
            stockObject.fromText(line);
            recordHid[cnt] = ((HTMPoint) stockObject).HTMid;

            for (int i = 0; i < numChild; i++) {
                long truncated = recordHid[cnt] >> (2 * (pointLevel - childDepths[i]));
                if (truncated == childHids[i]) {
                    childLen[i] += i_end - i_start - 1 - idLen + NEW_LINE.length;
                    break;
                }
            }

            i_start = i_end;
            cnt++;
        }

        int sum = 0;
        for (int i = 0; i < numChild; i++) {
            sum += childLen[i];
        }

        int childPtr[] = new int[numChild];
        childPtr[0] = 0;
        for (int i = 1; i < numChild; i++) {
            childPtr[i] = childPtr[i-1] + childLen[i-1];
        }

        byte[] content = new byte[sum];

        cnt = 0;
        i_start = offset;

        while (i_start < offset + len) {
            int i_end = skipToEOL(element_bytes, i_start);
            line.set(element_bytes, i_start, i_end - i_start - 1);
            String s = line.toString();
            int idLen = s.split(",")[0].length() + 1;
            long pointId = recordHid[cnt];

            for (int i = 0; i < numChild; i++) {
                long truncated = pointId >> (2 * (pointLevel - childDepths[i]));
                if (truncated == childHids[i]) {
                    System.arraycopy(element_bytes, i_start + idLen, content, childPtr[i],
                            i_end - i_start - 1 - idLen);
                    childPtr[i] += i_end - i_start - 1 - idLen;
                    System.arraycopy(NEW_LINE, 0, content, childPtr[i], NEW_LINE.length);
                    childPtr[i] += NEW_LINE.length;
                    break;
                }
            }
            i_start = i_end;
            cnt++;
        }



        dataOut.writeInt(numChild);
        int bytesCnt = 0;
        bytesCnt += 4 + 12 * numChild;
        for (int i = 0; i < numChild; i++) {
            dataOut.writeLong(childHids[i]);
            dataOut.writeInt(bytesCnt);
            bytesCnt += childLen[i];
        }
        LOG.info(sum);

        i_start = 0;
        cnt = 1;
        while (i_start < sum) {
            int i_end = skipToEOL(content, i_start);
            dataOut.write(content, i_start, i_end - i_start);
            i_start = i_end;
            cnt++;
        }
    }
}
