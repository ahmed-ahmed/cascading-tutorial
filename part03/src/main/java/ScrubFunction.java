import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

public class ScrubFunction extends BaseOperation implements Function {
    public ScrubFunction(Fields fieldDeclaration) {
        super(1, fieldDeclaration);
    }

    public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
        TupleEntry args = functionCall.getArguments();
//        String doc_id = args.getString( 0 );
        String token = scrubText( args.getString( 0 ) );

        Tuple result = new Tuple();
//        result.add(doc_id);
        result.add(token);

        functionCall.getOutputCollector().add(result);

    }

    private String scrubText(String text) {
        return text.trim().toUpperCase();
    }
}
