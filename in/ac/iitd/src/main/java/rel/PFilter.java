package rel;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.List;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexInterpreter;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.commons.math3.util.Decimal64;

import convention.PConvention;


public class PFilter extends Filter implements PRel {

    public PFilter(
            RelOptCluster cluster,
            RelTraitSet traits,
            RelNode child,
            RexNode condition) {
        super(cluster, traits, child, condition);
        assert getConvention() instanceof PConvention;
    }

    private Object[] current_row ;

    @Override
    public Filter copy(RelTraitSet traitSet, RelNode input, RexNode condition) {
        return new PFilter(getCluster(), traitSet, input, condition);
    }

    @Override
    public String toString() {
        return "PFilter";
    }

    // returns true if successfully opened, false otherwise
    @Override
    public boolean open(){
        logger.trace("Opening PFilter");
        /* Write your code here */
        PRel input_1 = (PRel) getInput();
        current_row = null;
        return input_1.open();

        // return false;
    }

    // any postprocessing, if needed
    @Override
    public void close(){
        logger.trace("Closing PFilter");
        /* Write your code here */
        PRel input_1 = (PRel) getInput();
        input_1.close();

        return;
    }

    // returns true if there is a next row, false otherwise
    @Override
    public boolean hasNext(){
        logger.trace("Checking if PFilter has next");
        /* Write your code here */
        Object[] inputrow;
        while (((PRel)getInput()).hasNext()){
            inputrow = ((PRel)getInput()).next();
            if (evaluatePredicate(inputrow)){
                current_row = inputrow;
                return true;
            }
        }
        return false;
    }

    @Override
    public Object[] next() {
        logger.trace("Getting next row from PFilter");
        return current_row;
    }

    private boolean evaluatePredicate(Object[] inputRow) {
        logger.trace("inside the evaluate predicate function of the filter operator");
        RexNode condition = getCondition();
        return (boolean) evaluate(condition, inputRow);
    }

    private Object evaluate(RexNode expression, Object[] inputRow) {
        if (expression instanceof RexLiteral) {
            RexLiteral literal = (RexLiteral) expression;
            switch (literal.getType().getSqlTypeName()){
                case CHAR:
                    return literal.getValueAs(String.class);
                case VARCHAR:
                    return literal.getValueAs(String.class);
                case DECIMAL:
                    return literal.getValueAs(Double.class);
                case INTEGER:
                    return literal.getValueAs(Integer.class);
                case DOUBLE:
                    return literal.getValueAs(Double.class);
                case FLOAT:
                    return literal.getValueAs(Float.class);
                case BOOLEAN:
                    return literal.getValueAs(Boolean.class);
                default:
                    return null;
            }
        } 
        // else if (expression instanceof RexInputRef) {
        //     int index = ((RexInputRef) expression).getIndex();
        //     return inputRow[index];
        else if (expression instanceof RexInputRef) {
            int index = ((RexInputRef) expression).getIndex();
            Object value = inputRow[index];
            // Convert to appropriate type if necessary
            if (value instanceof String) {
                return (String) value;
            } else if (value instanceof Double) {
                return (Double) value;
            } else if (value instanceof Integer) {
                return (Integer) value;
            } else if (value instanceof Float) {
                return (Float) value;
            } else if (value instanceof Boolean) {
                return (Boolean) value;
            } else if (value instanceof BigDecimal) {
                return (Double) value;
            } else {
                return null; // Unsupported data type
            }
        }
        else if (expression instanceof RexCall) {
            RexCall call = (RexCall) expression;
            List<RexNode> operands = call.getOperands();
            switch (call.getOperator().getName()) {
                case "AND": // Logical AND operator
                    if ((evaluate(operands.get(0), inputRow) == null) || ( evaluate(operands.get(1), inputRow) == null)){
                        return null;
                    }
                    return (boolean) evaluate(operands.get(0), inputRow) && (boolean) evaluate(operands.get(1), inputRow);
                case "OR": // Logical OR operator
                    if ((evaluate(operands.get(0), inputRow) == null) || ( evaluate(operands.get(1), inputRow) == null)){
                        return null;
                    }
                    return (boolean) evaluate(operands.get(0), inputRow) || (boolean) evaluate(operands.get(1), inputRow);
                case "NOT": // Logical NOT operator
                    if ((evaluate(operands.get(0), inputRow) == null) || ( evaluate(operands.get(1), inputRow) == null)){
                        return null;
                    }
                    return !(boolean) evaluate(operands.get(0), inputRow);
                case "<": // Less than operator
                    return compare(operands, inputRow) < 0;
                case "<=": // Less than or equal to operator
                    return compare(operands, inputRow) <= 0;
                case ">": // Greater than operator
                    return compare(operands, inputRow) > 0;
                case ">=": // Greater than or equal to operator
                    return compare(operands, inputRow) >= 0;
                case "=": // Equal to operator
                    return compare(operands, inputRow) == 0;
                case "!=": // Not equal to operator
                    return compare(operands, inputRow) != 0;
                case "+": // Addition operator
                    return arithmeticOperation(operands, inputRow, "+");
                case "-": // Subtraction operator
                    return arithmeticOperation(operands, inputRow, "-");
                case "*": // Multiplication operator
                    return arithmeticOperation(operands, inputRow, "*");
                case "/": // Division operator
                    return arithmeticOperation(operands, inputRow, "/");
                default:
                    return null; // Unsupported operator, return false
            }
        } else {
            return null; // Unsupported expression type, return false
        }
    }
    
    private int compare(List<RexNode> operands, Object[] inputRow) {
        Comparable<Object> left = (Comparable<Object>) evaluate(operands.get(0), inputRow);
        Comparable<Object> right = (Comparable<Object>) evaluate(operands.get(1), inputRow);
        
        if (left == null && right == null) {
            // Both operands are null, consider them equal
            return 0;
        } else if (left == null) {
            // Left operand is null, consider it less than right operand
            return -1;
        } else if (right == null) {
            // Right operand is null, consider it greater than left operand
            return 1;
        } else {
            // Both operands are non-null, compare them
            return left.compareTo(right);
        }
    }

    private Object arithmeticOperation(List<RexNode> operands, Object[] inputRow, String operator) {
        Object left = evaluate(operands.get(0), inputRow);
        Object right = evaluate(operands.get(1), inputRow);

        if (left == null || right == null){
            return null;
        }
        
        if (left instanceof Integer && right instanceof Integer) {
            switch (operator) {
                case "+":
                    return (Integer) left + (Integer) right;
                case "-":
                    return (Integer) left - (Integer) right;
                case "*":
                    return (Integer) left * (Integer) right;
                case "/":
                    return (Integer) left / (Integer) right;
                default:
                    return null;
            }
        } else if (left instanceof Double || right instanceof Double) {
            double leftDouble = left instanceof Integer ? (double) (Integer) left : (double) left;
            double rightDouble = right instanceof Integer ? (double) (Integer) right : (double) right;
            switch (operator) {
                case "+":
                    return leftDouble + rightDouble;
                case "-":
                    return leftDouble - rightDouble;
                case "*":
                    return leftDouble * rightDouble;
                case "/":
                    return leftDouble / rightDouble;
                default:
                    return null;
            }
        } else if (left instanceof Float || right instanceof Float) {
            float leftFloat = left instanceof Integer ? (float) (Integer) left : (float) left;
            float rightFloat = right instanceof Integer ? (float) (Integer) right : (float) right;
            switch (operator) {
                case "+":
                    return leftFloat + rightFloat;
                case "-":
                    return leftFloat - rightFloat;
                case "*":
                    return leftFloat * rightFloat;
                case "/":
                    return leftFloat / rightFloat;
                default:
                    return null;
            }
        } else if (left instanceof BigDecimal || right instanceof BigDecimal) {
            BigDecimal leftDecimal = left instanceof BigDecimal ? (BigDecimal) left : BigDecimal.valueOf((Integer) left);
            BigDecimal rightDecimal = right instanceof BigDecimal ? (BigDecimal) right : BigDecimal.valueOf((Integer) right);
            switch (operator) {
                case "+":
                    return leftDecimal.add(rightDecimal);
                case "-":
                    return leftDecimal.subtract(rightDecimal);
                case "*":
                    return leftDecimal.multiply(rightDecimal);
                case "/":
                    if (rightDecimal.compareTo(BigDecimal.ZERO) == 0) {
                        return null; 
                    }
                    return leftDecimal.divide(rightDecimal, 10, RoundingMode.HALF_UP);
                default:
                    return null;
            }
        } else {
            return null; 
        }
    }



}
