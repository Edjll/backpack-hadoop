package ru.edjll.hadoop.type.map.input;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class ValueInputMapType implements Writable {

    private List<Text> items;
    private List<Integer> counts;
    private List<Double> weights;
    private List<Double> costs;

    private double limit;

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(this.items.size());
        for (Text item : this.items) {
            item.write(dataOutput);
        }

        dataOutput.writeInt(this.counts.size());
        for (Integer count : this.counts) {
            dataOutput.writeInt(count);
        }

        dataOutput.writeInt(this.weights.size());
        for (Double weight : this.weights) {
            dataOutput.writeDouble(weight);
        }

        dataOutput.writeInt(this.costs.size());
        for (Double cost : this.costs) {
            dataOutput.writeDouble(cost);
        }

        dataOutput.writeDouble(limit);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        int itemsSize = dataInput.readInt();
        this.items = new ArrayList<>(itemsSize);
        for (int i = 0; i < itemsSize; i++) {
            Text text = new Text();
            text.readFields(dataInput);
            this.items.add(text);
        }

        int countsSize = dataInput.readInt();
        this.counts = new ArrayList<>(countsSize);
        for (int i = 0; i < countsSize; i++) {
            this.counts.add(dataInput.readInt());
        }

        int weightsSize = dataInput.readInt();
        this.weights = new ArrayList<>(weightsSize);
        for (int i = 0; i < weightsSize; i++) {
            this.weights.add(dataInput.readDouble());
        }

        int costsSize = dataInput.readInt();
        this.costs = new ArrayList<>(costsSize);
        for (int i = 0; i < costsSize; i++) {
            this.costs.add(dataInput.readDouble());
        }

        this.limit = dataInput.readDouble();
    }
}
