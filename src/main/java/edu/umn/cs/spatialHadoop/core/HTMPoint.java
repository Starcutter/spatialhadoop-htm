package edu.umn.cs.spatialHadoop.core;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;

import edu.umn.cs.spatialHadoop.io.TextSerializerHelper;

public class HTMPoint extends Point {
    public long HTMid;

    public HTMPoint() {
    }

    public HTMPoint(Point p, long HTMid) {
        set(p.x, p.y);
        this.HTMid = HTMid;
    }

    public void write(DataOutput out) throws IOException {
        out.writeLong(HTMid);
        super.write(out);
    }

    public void readFields(DataInput in) throws IOException {
        HTMid = in.readLong();
        super.readFields(in);
    }

    public void fromText(Text text) {
        HTMid = TextSerializerHelper.consumeLong(text, ',');
        super.fromText(text);
    }

    public Text toText(Text text) {
        TextSerializerHelper.serializeLong(HTMid, text, ',');
        return super.toText(text);
    }

    public HTMPoint clone() {
        HTMPoint c = new HTMPoint();
        c.HTMid = this.HTMid; // Set the id field
        c.x = this.x;
        c.y = this.y;
        return c;
    }
}
