package ru.edjll.hadoop.type.map.output;

import lombok.Getter;
import lombok.NoArgsConstructor;
import ru.edjll.hadoop.type.map.input.ValueInputMapType;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

@Getter
@NoArgsConstructor
public class ValueOutputMapType extends ValueInputMapType {

    private int[] combination;

    public ValueOutputMapType(ValueOutputMapType valueOutputMapType) {
        setItems(valueOutputMapType.getItems());
        setWeights(valueOutputMapType.getWeights());
        setCosts(valueOutputMapType.getCosts());
        setCounts(valueOutputMapType.getCounts());
        setLimit(valueOutputMapType.getLimit());

        this.combination = valueOutputMapType.getCombination();
    }

    public ValueOutputMapType(ValueInputMapType valueInputMapType, int[] combination) {
        setItems(valueInputMapType.getItems());
        setWeights(valueInputMapType.getWeights());
        setCosts(valueInputMapType.getCosts());
        setCounts(valueInputMapType.getCounts());
        setLimit(valueInputMapType.getLimit());

        this.combination = combination;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        super.write(dataOutput);

        dataOutput.writeInt(this.combination.length);
        for (int item : this.combination) {
            dataOutput.writeInt(item);
        }
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        super.readFields(dataInput);

        int combinationSize = dataInput.readInt();
        this.combination = new int[combinationSize];
        for (int i = 0; i < combinationSize; i++) {
            this.combination[i] = dataInput.readInt();
        }
    }

    @Override
    public String toString() {
        StringBuilder stringBuilder = new StringBuilder("Item\tWeight\tCost\tCount\n");
        for (int i = 0; i < getItems().size(); i++) {
            stringBuilder
                    .append("\t")
                    .append(getItems().get(i))
                    .append(" \t")
                    .append(getWeights().get(i))
                    .append(" \t")
                    .append(getCosts().get(i))
                    .append(" \t")
                    .append(getCombination()[i])
                    .append("\n");
        }
        return stringBuilder.toString();
    }
}
