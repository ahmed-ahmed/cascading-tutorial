import cascading.flow.Flow;
import cascading.flow.FlowDef;
import cascading.flow.local.LocalFlowConnector;
import cascading.pipe.Pipe;
import cascading.scheme.Scheme;
import cascading.scheme.local.TextDelimited;
import cascading.tap.Tap;
import cascading.tap.local.FileTap;
import cascading.tuple.Fields;

public class MyApp {
    public static void main(String[] args) {

        // in tap
        String inPath = args[0];
        Fields fields = new Fields("line");
        Scheme inScheme = new TextDelimited(fields);
        Tap inTap = new FileTap(inScheme, inPath);

        // specify a pipe to connect the taps
        Pipe copyPipe = new Pipe( "copy" );


        // out sink
        String outPath = args[1];
        Scheme outScheme = new TextDelimited(fields);
        Tap outTap = new FileTap(outScheme, outPath);



        FlowDef flowDef = FlowDef.flowDef()
                .addSource(copyPipe, inTap)
                .addTailSink(copyPipe, outTap)
                .setName("copy");


        Flow flow = new LocalFlowConnector().connect(flowDef);
        flow.complete();
    }
}