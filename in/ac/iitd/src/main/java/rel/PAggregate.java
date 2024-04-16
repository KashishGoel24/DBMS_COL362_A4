package rel;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.util.ImmutableBitSet;

import convention.PConvention;
import edu.emory.mathcs.backport.java.util.Arrays;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

// Count, Min, Max, Sum, Avg
public class PAggregate extends Aggregate implements PRel {

    public PAggregate(
            RelOptCluster cluster,
            RelTraitSet traitSet,
            List<RelHint> hints,
            RelNode input,
            ImmutableBitSet groupSet,
            List<ImmutableBitSet> groupSets,
            List<AggregateCall> aggCalls) {
        super(cluster, traitSet, hints, input, groupSet, groupSets, aggCalls);
        assert getConvention() instanceof PConvention;
    }

    @Override
    public Aggregate copy(RelTraitSet traitSet, RelNode input, ImmutableBitSet groupSet,
                          List<ImmutableBitSet> groupSets, List<AggregateCall> aggCalls) {
        return new PAggregate(getCluster(), traitSet, hints, input, groupSet, groupSets, aggCalls);
    }

    @Override
    public String toString() {
        return "PAggregate";
    }

    private List<Object[]> allInputRows = new ArrayList<>();
    private int curr_index = 0;
    private Map<List<Object>, List<Object[]>> hashTable = new HashMap<>(); 
    private List<Object> keyList = new ArrayList<>();

    // returns true if successfully opened, false otherwise
    @Override
    public boolean open() {
        logger.trace("Opening PAggregate");
        /* Write your code here */
        PRel input_1 = (PRel) getInput();
        if (input_1.open()){
            List<Integer> indices = groupSet.asList();
            groupRows(input_1, indices);
            List<AggregateCall> aggcalls = getAggCallList();
            aggregateFunction(aggcalls);
            return true;
        }
        return false;
    }

    public void groupRows(PRel input_left, List<Integer> indices){
        Object[] inputRow;

        while (input_left.hasNext()){
            inputRow = input_left.next();
            List<Object> key = getKey(indices, inputRow);
            hashTable.computeIfAbsent(key, k -> new ArrayList<>()).add(inputRow);
        }
        return;
    }

    private List<Object> getKey(List<Integer> indices, Object[] inputrow){
        List<Object> key = new ArrayList<>();
        for (int i = 0 ; i < indices.size() ; i++){
            key.add(inputrow[indices.get(i)]);
        }
        if (!keyList.contains(key)) {
            keyList.add(key);
        }
        return key;
    }

    private void aggregateFunction(List<AggregateCall> aggcalls) {
        for (Map.Entry<List<Object>, List<Object[]>> entry : hashTable.entrySet()) {
            List<Object> key = entry.getKey();
            List<Object[]> rows = entry.getValue();
            
            Object[] outputRow = new Object[key.size() + aggcalls.size()];
    
            // Set group keys in the output row
            for (int i = 0; i < key.size(); i++) {
                outputRow[i] = key.get(i);
            }
    
            // Apply aggregate functions
            for (int i = 0; i < aggcalls.size(); i++) {
                AggregateCall aggCall = aggcalls.get(i);
                List<Integer> argList = aggCall.getArgList();
    
                switch (aggCall.getAggregation().getName().toUpperCase()) {
                    case "SUM":
                        int sum = 0;
                        for (Object[] row : rows) {
                            System.out.println(row[argList.get(0)]);
                            sum += (Integer) row[argList.get(0)];
                        }
                        outputRow[key.size() + i] = sum;
                        break;
                    case "AVG":
                        long total = 0;
                        for (Object[] row : rows) {
                            total += (long) row[argList.get(0)];
                        }
                        outputRow[key.size() + i] = (double) total / rows.size();
                        break;
                    case "COUNT":
                        outputRow[key.size() + i] = rows.size();
                        break;
                    case "MAX":
                        Object max = null;
                        for (Object[] row : rows) {
                            Object value = row[argList.get(0)];
                            if (max == null || ((Comparable<Object>) value).compareTo(max) > 0) {
                                max = value;
                            }
                        }
                        outputRow[key.size() + i] = max;
                        break;
                    case "MIN":
                        Object min = null;
                        for (Object[] row : rows) {
                            Object value = row[argList.get(0)];
                            if (min == null || ((Comparable<Object>) value).compareTo(min) < 0) {
                                min = value;
                            }
                        }
                        outputRow[key.size() + i] = min;
                        break;
                    default:
                        outputRow[key.size() + i] = "Unsupported";
                        break;
                }
            }
            allInputRows.add(outputRow);
        }
    }
    
    

    // any postprocessing, if needed
    @Override
    public void close() {
        logger.trace("Closing PAggregate");
        /* Write your code here */
        PRel input_1 = (PRel) getInput();
        input_1.close();
        return;
    }

    // returns true if there is a next row, false otherwise
    @Override
    public boolean hasNext() {
        logger.trace("Checking if PAggregate has next");
        /* Write your code here */
        if (curr_index < allInputRows.size()){
            return true;
        }
        
        return false;
    }

    // returns the next row
    @Override
    public Object[] next() {
        logger.trace("Getting next row from PAggregate");
        if (curr_index < allInputRows.size()){
            curr_index += 1;
            return allInputRows.get(curr_index-1);
        }
        return null;
    }

}