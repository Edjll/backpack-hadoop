package ru.edjll.hadoop.format.input;

import lombok.Getter;
import lombok.NoArgsConstructor;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.UTF8;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

@Getter
@NoArgsConstructor
public class BackpackInputSplit extends InputSplit implements Writable {

    private int firstItemIndex;
    private int firstItemCount;

    private int secondItemIndex;
    private int secondItemCount;

    private Path path;

    private double limit;

    public BackpackInputSplit(int firstItem, int firstItemSize, int secondItem, int secondItemSize, Path path, double limit) {
        this.firstItemIndex = firstItem;
        this.firstItemCount = firstItemSize;
        this.secondItemIndex = secondItem;
        this.secondItemCount = secondItemSize;
        this.path = path;
        this.limit = limit;
    }

    public long getLength() {
        return 0;
    }

    public String[] getLocations() {
        return new String[0];
    }

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(firstItemIndex);
        dataOutput.writeInt(firstItemCount);
        dataOutput.writeInt(secondItemIndex);
        dataOutput.writeInt(secondItemCount);
        UTF8.writeString(dataOutput, path.toString());
        dataOutput.writeDouble(limit);
    }

    public void readFields(DataInput dataInput) throws IOException {
        firstItemIndex = dataInput.readInt();
        firstItemCount = dataInput.readInt();
        secondItemIndex = dataInput.readInt();
        secondItemCount = dataInput.readInt();
        this.path = new Path(UTF8.readString(dataInput));
        limit = dataInput.readDouble();
    }
}
