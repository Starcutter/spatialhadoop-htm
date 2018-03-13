package edu.umn.cs.spatialHadoop.core;

import cn.edu.tsinghua.cs.htm.utils.HTMid;
import edu.umn.cs.spatialHadoop.io.TextSerializerHelper;
import org.apache.hadoop.io.Text;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class HTMidInfo {
    public HTMid htmId;

    /**
     * Loads a cell serialized to the given stream
     * @param in
     * @throws IOException
     */
    public HTMidInfo(DataInput in) throws IOException {
        this.readFields(in);
    }

    public HTMidInfo(String in) {
        this.fromText(new Text(in));
    }

    public HTMidInfo() {
    }

    public HTMidInfo(long id) {
        this.htmId = new HTMid(id);
    }

    public HTMidInfo(HTMid htmId) {
        this.htmId = new HTMid(htmId.getId());
    }

    public HTMidInfo(HTMidInfo c) {
        this.set(c);
    }

    public void set(HTMidInfo c) {
        if (c == null) {
            this.htmId = new HTMid(0); // Invalid number
        } else {
            this.htmId = new HTMid(c.htmId.getId());
        }
    }

    @Override
    public String toString() {
        return "HTMid #"+ htmId.getId();
    }

    @Override
    public HTMidInfo clone() {
        return new HTMidInfo(htmId);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null)
            return false;
        return ((HTMidInfo)obj).htmId == this.htmId;
    }

    public int compareTo(Shape s) {
        long tmp = this.htmId.getId() - ((HTMidInfo)s).htmId.getId();
        if (tmp > 0) {
            return 1;
        } else if (tmp < 0) {
            return -1;
        } else {
            return 0;
        }
    }

    public int hashCode() {
        return (int)(this.htmId.getId() % 2147483647);
    }

    public void write(DataOutput out) throws IOException {
        out.writeLong(htmId.getId());
    }

    public void readFields(DataInput in) throws IOException {
        this.htmId = new HTMid(in.readLong());
    }

    public Text toText(Text text) {
        TextSerializerHelper.serializeLong(htmId.getId(), text, '\0');
        return text;
    }

    public void fromText(Text text) {
        this.htmId = new HTMid(TextSerializerHelper.consumeLong(text, '\0'));
    }
}
