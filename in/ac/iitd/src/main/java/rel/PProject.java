package rel;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexInterpreter;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.rex.RexUtil;

import convention.PConvention;

import java.math.BigDecimal;
import java.util.List;

// Hint: Think about alias and arithmetic operations
public class PProject extends Project implements PRel {

    public PProject(
            RelOptCluster cluster,
            RelTraitSet traits,
            RelNode input,
            List<? extends RexNode> projects,
            RelDataType rowType) {
        super(cluster, traits, ImmutableList.of(), input, projects, rowType);
        assert getConvention() instanceof PConvention;
    }

    @Override
    public PProject copy(RelTraitSet traitSet, RelNode input,
                            List<RexNode> projects, RelDataType rowType) {
        return new PProject(getCluster(), traitSet, input, projects, rowType);
    }

    @Override
    public String toString() {
        return "PProject";
    }

    // returns true if successfully opened, false otherwise
    @Override
    public boolean open(){
        logger.trace("Opening PProject");

        /* Write your code here */
        PRel input_1 = (PRel) input;
        return input_1.open();       
        // return false;
    }

    // any postprocessing, if needed
    @Override
    public void close(){
        logger.trace("Closing PProject");
        /* Write your code here */
        PRel input_1 = (PRel) input;
        input_1.close();      
        return;
    }

    // returns true if there is a next row, false otherwise
    @Override
    public boolean hasNext(){
        logger.trace("Checking if PProject has next");
        /* Write your code here */
        PRel input_1 = (PRel) input;
        return input_1.hasNext();      

    }

    // returns the next row
    @Override
    public Object[] next(){
        logger.trace("Getting next row from PProject");
        // if (!((PRel)input).hasNext()) {
        //     return null;
        // }
        Object[] inputRow = ((PRel)input).next();
        if (inputRow == null){
            return null;
        }
        Object[] projectedRow = new Object[getProjects().size()];
        for (int i = 0; i < getProjects().size(); i++) {
           RexNode projectExpr = getProjects().get(i);
           projectedRow[i] = evaluate(projectExpr, inputRow);
        }
        return projectedRow;
    }

    private Object evaluate(RexNode expression, Object[] inputRow) {
        if (expression instanceof RexLiteral) {
            // Handle literal values
            RexLiteral literal = (RexLiteral) expression;
            switch (literal.getType().getSqlTypeName()) {
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
                    return null; // Unsupported literal type
            }
        } else if (expression instanceof RexInputRef) {
            // Handle input references
            int index = ((RexInputRef) expression).getIndex();
            Object value = inputRow[index];
            // Typecast the value to the appropriate type
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
            } else {
                return null; // Unsupported data type
            }
        } else if (expression instanceof RexCall) {
            // Handle function calls
            RexCall call = (RexCall) expression;
            List<RexNode> operands = call.getOperands();
            switch (call.getOperator().getName()) {
                case "+": // Addition operator
                    return arithmeticOperation(operands, inputRow, "+");
                case "-": // Subtraction operator
                    return arithmeticOperation(operands, inputRow, "-");
                case "*": // Multiplication operator
                    return arithmeticOperation(operands, inputRow, "*");
                case "/": // Division operator
                    return arithmeticOperation(operands, inputRow, "/");
                default:
                    return null; // Unsupported operator, return null
            }
        } else {
            return null; // Unsupported expression type, return null
        }
    }
    
    private Object arithmeticOperation(List<RexNode> operands, Object[] inputRow, String operator) {
        Object left = evaluate(operands.get(0), inputRow);
        Object right = evaluate(operands.get(1), inputRow);
        // Check operand types and perform operation accordingly
        if (left == null || right == null){
            return null;
        }

        if (left instanceof Integer && right instanceof Integer) {
            return performIntegerOperation((Integer) left, (Integer) right, operator);
        } else if (left instanceof Double && right instanceof Double) {
            return performDoubleOperation((Double) left, (Double) right, operator);
        } else if (left instanceof Float && right instanceof Float) {
            return performFloatOperation((Float) left, (Float) right, operator);
        } else if (left instanceof BigDecimal && right instanceof BigDecimal) {
            return performMixedOperation((Double) left, (Double) right, operator);
        } else if (left instanceof Double && right instanceof Integer) {
            return performMixedOperation((Double) left, (Integer) right, operator);
        } else if (left instanceof Integer && right instanceof Float) {
            return performMixedOperation((Integer) left, (Float) right, operator);
        } else if (left instanceof Float && right instanceof Integer) {
            return performMixedOperation((Float) left, (Integer) right, operator);
        } else if (left instanceof Double && right instanceof Float) {
            return performMixedOperation((Double) left, (Float) right, operator);
        } else if (left instanceof Float && right instanceof Double) {
            return performMixedOperation((Float) left, (Double) right, operator);
        } else {
            return null; // Unsupported operand types
        }
    }
    
    private Object performIntegerOperation(Integer left, Integer right, String operator) {
        switch (operator) {
            case "+":
                return left + right;
            case "-":
                return left - right;
            case "*":
                return left * right;
            case "/":
                if (right != 0) {
                    return left / right;
                } else {
                    return null; // Division by zero
                }
            default:
                return null; // Unsupported operator
        }
    }
    
    private Object performDoubleOperation(Double left, Double right, String operator) {
        switch (operator) {
            case "+":
                return left + right;
            case "-":
                return left - right;
            case "*":
                return left * right;
            case "/":
                if (right != 0) {
                    return left / right;
                } else {
                    return null; // Division by zero
                }
            default:
                return null; // Unsupported operator
        }
    }
    
    private Object performFloatOperation(Float left, Float right, String operator) {
        switch (operator) {
            case "+":
                return left + right;
            case "-":
                return left - right;
            case "*":
                return left * right;
            case "/":
                if (right != 0) {
                    return left / right;
                } else {
                    return null; // Division by zero
                }
            default:
                return null; // Unsupported operator
        }
    }
    
    private Object performMixedOperation(Number left, Number right, String operator) {
        switch (operator) {
            case "+":
                return left.doubleValue() + right.doubleValue();
            case "-":
                return left.doubleValue() - right.doubleValue();
            case "*":
                return left.doubleValue() * right.doubleValue();
            case "/":
                if (right.doubleValue() != 0) {
                    return left.doubleValue() / right.doubleValue();
                } else {
                    return null; // Division by zero
                }
            default:
                return null; // Unsupported operator
        }
    }
    
    
    // private Object evaluate(RexNode expression, Object[] inputRow) {
    //     if (expression instanceof RexLiteral) {
    //         // need to make cases here for the 5 types of attribute types
    //         return ((RexLiteral) expression).getValue();
    //     } else if (expression instanceof RexInputRef) {
    //         // check if there is a need to make the cases here 
    //         int index = ((RexInputRef) expression).getIndex();
    //         return inputRow[index];
    //     } else if (expression instanceof RexCall) {
    //         RexCall call = (RexCall) expression;
    //         List<RexNode> operands = call.getOperands();
    //         switch (call.getOperator().getName()) {
    //             // ccheck if there is a need to make cases for strings and make separate cases for integer, float, double
    //             // chekc if any more operatos needed to be added in here 
    //             case "+":       
    //                 Object leftAdd = evaluate(operands.get(0), inputRow);
    //                 Object rightAdd = evaluate(operands.get(1), inputRow);
    //                 if (leftAdd instanceof Number && rightAdd instanceof Number) {
    //                     return ((Number) leftAdd).doubleValue() + ((Number) rightAdd).doubleValue();
    //                 } else {
    //                     return null; 
    //                 }
    //             case "-":     
    //                 Object leftSub = evaluate(operands.get(0), inputRow);
    //                 Object rightSub = evaluate(operands.get(1), inputRow);
    //                 if (leftSub instanceof Number && rightSub instanceof Number) {
    //                     return ((Number) leftSub).doubleValue() - ((Number) rightSub).doubleValue();
    //                 } else {
    //                     return null; 
    //                 }
    //             case "*":
    //                 Object leftMul = evaluate(operands.get(0), inputRow);
    //                 Object rightMul = evaluate(operands.get(1), inputRow);
    //                 if (leftMul instanceof Number && rightMul instanceof Number) {
    //                     return ((Number) leftMul).doubleValue() * ((Number) rightMul).doubleValue();
    //                 } else {
    //                     return null; 
    //                 }
    //             case "/":         
    //                 Object leftDiv = evaluate(operands.get(0), inputRow);
    //                 Object rightDiv = evaluate(operands.get(1), inputRow);
    //                 if (leftDiv instanceof Number && rightDiv instanceof Number) {
    //                     double divisor = ((Number) rightDiv).doubleValue();
    //                     if (divisor != 0) {
    //                         return ((Number) leftDiv).doubleValue() / divisor;
    //                     } else {
    //                         return null;
    //                     }
    //                 } else {
    //                     return null; 
    //                 }
    //             default:
    //                 return null; // Unsupported operator, return null
    //         }
    //     } else {
    //         return null; // Unsupported expression type, return null
    //     }
    // }
    


}
