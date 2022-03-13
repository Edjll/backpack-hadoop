package ru.edjll.hadoop.type.map.input;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

@Getter
@Setter
@AllArgsConstructor
public class KeyInputMapType implements WritableComparable<KeyInputMapType> {

    private int firstItemIndex;
    private int firstItemCount;

    private int secondItemIndex;
    private int secondItemCount;

    private double cost;

    public KeyInputMapType() {
    }

    public KeyInputMapType(double cost) {
        this.cost = cost;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(firstItemIndex);
        dataOutput.writeInt(firstItemCount);
        dataOutput.writeInt(secondItemIndex);
        dataOutput.writeInt(secondItemCount);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        firstItemIndex = dataInput.readInt();
        firstItemCount = dataInput.readInt();
        secondItemIndex = dataInput.readInt();
        secondItemCount = dataInput.readInt();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        KeyInputMapType that = (KeyInputMapType) o;
        return firstItemIndex == that.firstItemIndex
                && firstItemCount == that.firstItemCount
                && secondItemIndex == that.secondItemIndex
                && secondItemCount == that.secondItemCount;
    }

    @Override
    public int hashCode() {
        return Objects.hash(firstItemIndex, firstItemCount, secondItemIndex, secondItemCount);
    }

    @Override
    public int compareTo(KeyInputMapType o) {
        return (this.firstItemIndex + 1) * (this.firstItemCount + 1) - (o.getFirstItemIndex() + 1) * (o.getFirstItemCount() + 1);
    }

    @Override
    public String toString() {
        return " ";
    }
}
