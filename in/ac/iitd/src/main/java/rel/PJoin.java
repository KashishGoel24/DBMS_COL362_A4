package rel;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;

import convention.PConvention;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/*
    * Implement Hash Join
    * The left child is blocking, the right child is streaming
*/
public class PJoin extends Join implements PRel {

    public PJoin(
            RelOptCluster cluster,
            RelTraitSet traitSet,
            RelNode left,
            RelNode right,
            RexNode condition,
            Set<CorrelationId> variablesSet,
            JoinRelType joinType) {
                super(cluster, traitSet, ImmutableList.of(), left, right, condition, variablesSet, joinType);
                assert getConvention() instanceof PConvention;
    }

    @Override
    public PJoin copy(
            RelTraitSet relTraitSet,
            RexNode condition,
            RelNode left,
            RelNode right,
            JoinRelType joinType,
            boolean semiJoinDone) {
        return new PJoin(getCluster(), relTraitSet, left, right, condition, variablesSet, joinType);
    }

    @Override
    public String toString() {
        return "PJoin";
    }

    private List<Object[]> allInputRows = new ArrayList<>();
    private int curr_index = 0;
    private Map<List<Object>, List<Object[]>> lefthashTable = new HashMap<>();
    private Map<List<Object>, List<Object[]>> righthashTable = new HashMap<>();
    private List<Object[]> leftRecordsArray = new ArrayList<>();
    private List<Object[]> rightRecordsArray = new ArrayList<>();
    private int leftrecordSize ;
    private int rightrecordSize;

    // returns true if successfully opened, false otherwise
    @Override
    public boolean open() {
        logger.trace("Opening PJoin");
        /* Write your code here */
        PRel input_left = (PRel) getLeft();
        PRel input_right = (PRel) getRight();
        if((!input_left.open()) || !input_right.open()){
            return false;
        }
        Integer[] leftAttributes =  joinInfo.leftKeys.toArray(new Integer[0]);
        Integer[] rightAttributes =  joinInfo.rightKeys.toArray(new Integer[0]);
        
        buildHashTable(input_left, leftAttributes,lefthashTable, leftRecordsArray, 1);
        buildHashTable(input_right, rightAttributes, righthashTable, rightRecordsArray, 0);
        probe(input_right, rightAttributes, rightRecordsArray, input_left, leftAttributes, leftRecordsArray);
        return true;
    }

    public void buildHashTable(PRel input_left, Integer[] leftAttributes, Map<List<Object>, List<Object[]>> hashmap, List<Object[]> recordsarray, int recordsize){
        Object[] inputRow;
        while (input_left.hasNext()){
            inputRow = input_left.next();
            if (recordsize == 1){
                leftrecordSize = inputRow.length;
            }
            else{
                rightrecordSize = inputRow.length;
            }
            recordsarray.add(inputRow);
            List<Object> key = getKey(leftAttributes, inputRow);
            hashmap.computeIfAbsent(key, k -> new ArrayList<>()).add(inputRow);
        }
        return;
    }

    private List<Object> getKey(Integer[] attributeSet, Object[] inputrow){
        List<Object> key = new ArrayList<>();
        for (int i = 0 ; i < attributeSet.length ; i++){
            key.add(inputrow[attributeSet[i]]);
        }
        return key;
    }

    public void probe(PRel input_right, Integer[] rightAttributes, List<Object[]> rightrecords, PRel input_left, Integer[] leftAttributes, List<Object[]> leftrecords) {
        // Object[] inputRow;

        // while (input_right.hasNext()) {
        //     inputRow = input_right.next();
        //     List<Object> key = getKey(rightAttributes, inputRow);
        //     List<Object[]> matchingLeftRows = hashTable.get(key);
            
        //     if (matchingLeftRows != null){
        //         for (Object[] leftRow : matchingLeftRows) {
        //             Object[] resultRow;
        //             resultRow = joinRows(leftRow, inputRow, getJoinType());
        //             allInputRows.add(resultRow);
        //         }
        //     }
        // }

        if (joinType == JoinRelType.LEFT){
            Object[] leftrow ;
            for (int i = 0 ; i < leftRecordsArray.size() ; i++){
                leftrow = leftRecordsArray.get(i);
                List<Object> lefthash = getKey(leftAttributes, leftrow);
                if (!righthashTable.containsKey(lefthash)){
                    Object[] resultRow = new Object[leftrecordSize + rightrecordSize];
                    for (int j = 0 ; j < rightrecordSize ; ++j){
                        resultRow[j+leftrecordSize] = null;
                    }
                    System.arraycopy(leftrow, 0, resultRow, 0, leftrecordSize);
                    allInputRows.add(resultRow);
                }
                else{
                    List<Object[]> matchingLeftRows = righthashTable.get(lefthash);
                    for (Object[] rightRow : matchingLeftRows){
                        Object[] resultRow = new Object[leftrecordSize + rightrecordSize];
                        System.arraycopy(leftrow, 0, resultRow, 0, leftrecordSize);
                        System.arraycopy(rightRow, 0, resultRow, leftrecordSize, rightrecordSize);
                        allInputRows.add(resultRow);
                    }
                }
            }
        }
        else if (joinType == JoinRelType.RIGHT){
            Object[] rightrow ;
            for (int i = 0 ; i < rightRecordsArray.size() ; i++){
                rightrow = rightRecordsArray.get(i);
                List<Object> righthash = getKey(rightAttributes, rightrow);
                if (!lefthashTable.containsKey(righthash)){
                    Object[] resultRow = new Object[leftrecordSize + rightrecordSize];
                    for (int j = 0 ; j < leftrecordSize ; ++j){
                        resultRow[j] = null;
                    }
                    System.arraycopy(rightrow, 0, resultRow, leftrecordSize, rightrecordSize);

                    allInputRows.add(resultRow);
                }
                else{
                    List<Object[]> matchingRightRows = lefthashTable.get(righthash);
                    for (Object[] leftrow : matchingRightRows){
                        Object[] resultRow = new Object[leftrecordSize + rightrecordSize];
                        System.arraycopy(leftrow, 0, resultRow, 0, leftrecordSize);
                        System.arraycopy(rightrow, 0, resultRow, leftrecordSize, rightrecordSize);
                        allInputRows.add(resultRow);
                    }
                }
            }
        }
        else if (joinType == JoinRelType.FULL){
            Object[] leftrow ;
            for (int i = 0 ; i < leftRecordsArray.size() ; i++){
                leftrow = leftRecordsArray.get(i);
                List<Object> lefthash = getKey(leftAttributes, leftrow);
                if (!righthashTable.containsKey(lefthash)){
                    Object[] resultRow = new Object[leftrecordSize + rightrecordSize];
                    for (int j = 0 ; j < rightrecordSize ; ++j){
                        resultRow[j+leftrecordSize] = null;
                    }
                    System.arraycopy(leftrow, 0, resultRow, 0, leftrecordSize);
                    allInputRows.add(resultRow);
                }
                else{
                    List<Object[]> matchingLeftRows = righthashTable.get(lefthash);
                    for (Object[] rightRow : matchingLeftRows){
                        Object[] resultRow = new Object[leftrecordSize + rightrecordSize];
                        System.arraycopy(leftrow, 0, resultRow, 0, leftrecordSize);
                        System.arraycopy(rightRow, 0, resultRow, leftrecordSize, rightrecordSize);
                        allInputRows.add(resultRow);
                    }
                }
            }

            Object[] rightrow ;
            for (int i = 0 ; i < rightRecordsArray.size() ; i++){
                rightrow = rightRecordsArray.get(i);
                List<Object> righthash = getKey(rightAttributes, rightrow);
                if (!lefthashTable.containsKey(righthash)){
                    Object[] resultRow = new Object[leftrecordSize + rightrecordSize];
                    for (int j = 0 ; j < leftrecordSize ; ++j){
                        resultRow[j] = null;
                    }
                    System.arraycopy(rightrow, 0, resultRow, leftrecordSize, rightrecordSize);

                    allInputRows.add(resultRow);
                }
            }
        }
        else if (joinType == JoinRelType.INNER){
            Object[] rightrow ;
            for (int i = 0 ; i < rightRecordsArray.size() ; i++){
                rightrow = rightRecordsArray.get(i);
                List<Object> righthash = getKey(rightAttributes, rightrow);
                if (lefthashTable.containsKey(righthash)){
                    List<Object[]> matchingRightRows = lefthashTable.get(righthash);
                    for (Object[] leftrow : matchingRightRows){
                        Object[] resultRow = new Object[leftrecordSize + rightrecordSize];
                        System.arraycopy(leftrow, 0, resultRow, 0, leftrecordSize);
                        System.arraycopy(rightrow, 0, resultRow, leftrecordSize, rightrecordSize);
                        allInputRows.add(resultRow);
                    }
                }
            }
        }
        
        return;
    }

    // private Object[] joinRows(Object[] leftRow, Object[] rightRow, JoinRelType joinType) {
    //     int leftRowSize = leftRow.length;
    //     int rightRowSize = rightRow.length;
    //     Object[] resultRow;
    
    //     switch (joinType) {
    //         case INNER:
    //             // For INNER JOIN, combine rows only if there's a match
    //             if (leftRow != null && rightRow != null) {
    //                 resultRow = new Object[leftRowSize + rightRowSize];
    //                 System.arraycopy(leftRow, 0, resultRow, 0, leftRowSize);
    //                 System.arraycopy(rightRow, 0, resultRow, leftRowSize, rightRowSize);
    //                 return resultRow;
    //             }
    //             break;
    //         case LEFT:
    //             // For LEFT JOIN, add left row regardless of match, use NULLs for right columns if no match
    //             resultRow = new Object[leftRowSize + rightRowSize];
    //             System.arraycopy(leftRow, 0, resultRow, 0, leftRowSize);
    //             if (rightRow != null) {
    //                 System.arraycopy(rightRow, 0, resultRow, leftRowSize, rightRowSize);
    //             }
    //             return resultRow;
    //         case RIGHT:
    //             // For RIGHT JOIN, add right row regardless of match, use NULLs for left columns if no match
    //             resultRow = new Object[leftRowSize + rightRowSize];
    //             if (leftRow != null) {
    //                 System.arraycopy(leftRow, 0, resultRow, 0, leftRowSize);
    //             }
    //             System.arraycopy(rightRow != null ? rightRow : new Object[rightRowSize], 0, resultRow, leftRowSize, rightRowSize);
    //             return resultRow;
    //         case FULL:
    //             // For FULL JOIN, add both left and right rows, use NULLs if no match
    //             resultRow = new Object[leftRowSize + rightRowSize];
    //             if (leftRow != null) {
    //                 System.arraycopy(leftRow, 0, resultRow, 0, leftRowSize);
    //             }
    //             if (rightRow != null) {
    //                 System.arraycopy(rightRow, 0, resultRow, leftRowSize, rightRowSize);
    //             }
    //             return resultRow;
    //         default:
    //             // Handle other join types if needed
    //             break;
    //     }
    //     return null;
    // }

    // any postprocessing, if needed
    @Override
    public void close() {
        logger.trace("Closing PJoin");
        /* Write your code here */
        ((PRel)getLeft()).close();
        ((PRel)getRight()).close();
        lefthashTable.clear();
        righthashTable.clear();
        leftRecordsArray.clear();
        rightRecordsArray.clear();
        curr_index = 0;
        allInputRows.clear();
        leftrecordSize = 0;
        rightrecordSize = 0;
        return;
    }

    // returns true if there is a next row, false otherwise
    @Override
    public boolean hasNext() {
        logger.trace("Checking if PJoin has next");
        /* Write your code here */
        if (curr_index < allInputRows.size()){
            return true;
        }
        return false;
    }

    // returns the next row
    @Override
    public Object[] next() {
        logger.trace("Getting next row from PJoin");
        /* Write your code here */
        if (curr_index < allInputRows.size()){
            curr_index += 1;
            return allInputRows.get(curr_index-1);
        }
        return null;
    }
}
