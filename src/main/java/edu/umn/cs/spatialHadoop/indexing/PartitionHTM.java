package edu.umn.cs.spatialHadoop.indexing;

import cn.edu.tsinghua.cs.htm.utils.HTMid;
import edu.umn.cs.spatialHadoop.core.HTMidInfo;
import edu.umn.cs.spatialHadoop.io.TextSerializerHelper;
import org.apache.hadoop.io.Text;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class PartitionHTM extends HTMidInfo {
    /**Name of the file that contains the data*/
    public String filename;

    /**Total number of records in this partition*/
    public long recordCount;

    /**Total size of data in this partition in bytes (uncompressed)*/
    public long size;

    public PartitionHTM() {}

    public PartitionHTM(String filename, HTMidInfo htmIdInfo) {
        this.filename = filename;
        super.set(htmIdInfo);
    }

    public PartitionHTM(PartitionHTM other) {
        this.filename = other.filename;
        this.recordCount = other.recordCount;
        this.size = other.size;
        super.set(other);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);
        out.writeUTF(filename);
        out.writeLong(recordCount);
        out.writeLong(size);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        super.readFields(in);
        filename = in.readUTF();
        this.recordCount = in.readLong();
        this.size = in.readLong();
    }

    @Override
    public Text toText(Text text) {
        super.toText(text);
        text.append(new byte[] {','}, 0, 1);
        TextSerializerHelper.serializeLong(recordCount, text, ',');
        TextSerializerHelper.serializeLong(size, text, ',');
        byte[] temp = (filename == null? "" : filename).getBytes();
        text.append(temp, 0, temp.length);
        return text;
    }

    @Override
    public void fromText(Text text) {
        super.fromText(text);
        text.set(text.getBytes(), 1, text.getLength() - 1); // Skip comma
        this.recordCount = TextSerializerHelper.consumeLong(text, ',');
        this.size = TextSerializerHelper.consumeLong(text, ',');
        filename = text.toString();
    }

    @Override
    public PartitionHTM clone() {
        return new PartitionHTM(this);
    }

    @Override
    public int hashCode() {
        return filename.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null)
            return false;
        return this.filename.equals(((PartitionHTM)obj).filename);
    }

    public void invalidate() {
        htmId = new HTMid(0);
    }
}
