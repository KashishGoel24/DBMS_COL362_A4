package rel;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;

import convention.PConvention;
import edu.emory.mathcs.backport.java.util.Collections;

public class PSort extends Sort implements PRel{
    
    public PSort(
            RelOptCluster cluster,
            RelTraitSet traits,
            List<RelHint> hints,
            RelNode child,
            RelCollation collation,
            RexNode offset,
            RexNode fetch
            ) {
        super(cluster, traits, hints, child, collation, offset, fetch);
        assert getConvention() instanceof PConvention;
    }

    private List<Object[]> allInputRows = new ArrayList<>();
    private int curr_index = 0;

    @Override
    public Sort copy(RelTraitSet traitSet, RelNode input, RelCollation collation, RexNode offset, RexNode fetch) {
        return new PSort(getCluster(), traitSet, hints, input, collation, offset, fetch);
    }

    @Override
    public String toString() {
        return "PSort";
    }

    // returns true if successfully opened, false otherwise
    @Override
    public boolean open(){
        logger.trace("Opening PSort");
        /* Write your code here */
        PRel inputRel = (PRel) getInput();
        int offsetValue = offset != null ? RexLiteral.intValue(offset) : 0;
        curr_index = offsetValue;
        if (inputRel.open()) {
            Object[] inputRow;
            while (inputRel.hasNext()){
                inputRow = inputRel.next();
                allInputRows.add(inputRow);
            }
            RelCollation collation = getCollation();
            Comparator<Object[]> rowComparator = createRowComparator(collation);
            Collections.sort(allInputRows, rowComparator);
            return true;
        }
        return false;
    }

    private Comparator<Object[]> createRowComparator(RelCollation collation) {
        Comparator<Object[]> rowComparator = (row1, row2) -> {
            for (RelFieldCollation fieldCollation : collation.getFieldCollations()) {
                int columnIndex = fieldCollation.getFieldIndex();
                Comparable value1 = (Comparable) row1[columnIndex];
                Comparable value2 = (Comparable) row2[columnIndex];
    
                // Handle null values
                if (value1 == null && value2 == null) {
                    continue; // Both values are null, move to next field
                } else if (value1 == null) {
                    return fieldCollation.getDirection().shortString.equals("ASC") ? -1 : 1;
                } else if (value2 == null) {
                    return fieldCollation.getDirection().shortString.equals("ASC") ? 1 : -1;
                }
    
                int comparison = value1.compareTo(value2);
                if (comparison != 0) {
                    return fieldCollation.getDirection().shortString.equals("ASC") ? comparison : -comparison;
                }
            }
            return 0;
        };
        return rowComparator;
    }
    


    // any postprocessing, if needed
    @Override
    public void close(){
        logger.trace("Closing PSort");
        /* Write your code here */
        PRel input_1 = (PRel) getInput();
        input_1.close();  
        return;
    }

    // returns true if there is a next row, false otherwise
    @Override
    public boolean hasNext(){
        logger.trace("Checking if PSort has next");
        /* Write your code here */
        int fetchValue = fetch != null ? RexLiteral.intValue(fetch) : Integer.MAX_VALUE;
        int offsetValue = offset != null ? RexLiteral.intValue(offset) : 0;
        // currentIndex = offsetValue;
        if ((curr_index < allInputRows.size()) && (curr_index - offsetValue < fetchValue)){
            return true;
        }
        return false;
    }

    // returns the next row
    @Override
    public Object[] next(){
        logger.trace("Getting next row from PSort");
        /* Write your code here */
        if (curr_index < allInputRows.size()){
            curr_index += 1;
            return allInputRows.get(curr_index-1);
        }
        return null;
    }

}
