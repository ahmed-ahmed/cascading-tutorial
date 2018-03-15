import cascading.flow.Flow;
import cascading.flow.FlowDef;
import cascading.flow.local.LocalFlowConnector;
import cascading.operation.Debug;
import cascading.operation.aggregator.Count;
import cascading.operation.regex.RegexSplitGenerator;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.scheme.Scheme;
import cascading.scheme.local.TextDelimited;
import cascading.tap.Tap;
import cascading.tap.local.FileTap;
import cascading.tuple.Fields;

public class MyApp {
    public static void main(String[] args) {
        Tap inputTap = new FileTap(new TextDelimited(new Fields("lines"), "^!.*\n"), args[0]);

        Pipe wordsPipe = new Each("wordsPipe",  new RegexSplitGenerator(new Fields("word"), "[ \\\\[\\\\]\\\\(\\\\),.]"), new Fields("word"));

        wordsPipe = new Each(wordsPipe, new Debug("words"));

        wordsPipe = new GroupBy(wordsPipe, new Fields("word"));
        wordsPipe = new Every(wordsPipe, new Count(), new Fields("word", "count"));

        Tap outPutTap = new FileTap(new TextDelimited(), args[1]);

        FlowDef flowDef = FlowDef.flowDef()
                .addSource(wordsPipe, inputTap)
                .addTailSink(wordsPipe, outPutTap)
                .setName("words count");

        Flow flow = new LocalFlowConnector().connect(flowDef);
        flow.complete();


//        // create source and sink taps
//        Tap docTap = new FileTap( new TextDelimited(), args[0] );
//        Tap wcTap = new FileTap( new TextDelimited(), args[1] );
//
//        // specify a regex operation to split the "document" text lines into a token stream
//        Fields token = new Fields( "token" );
//        Fields text = new Fields( "text" );
//        RegexSplitGenerator splitter = new RegexSplitGenerator( token, "[ \\[\\]\\(\\),.]" );
//        // only returns "token"
//        Pipe docPipe = new Each( "token", splitter, Fields.RESULTS );
//
//        // determine the word counts
//        Pipe wcPipe = new Pipe( "wc", docPipe );
//        wcPipe = new GroupBy( wcPipe, token );
//        wcPipe = new Every( wcPipe, Fields.ALL, new Count(), Fields.ALL );
//
//        // connect the taps, pipes, etc., into a flow
//        FlowDef flowDef = FlowDef.flowDef().setName( "wc" ).addSource( docPipe, docTap ).addTailSink( wcPipe, wcTap );
//        Flow flow = new LocalFlowConnector().connect(flowDef);
//        flow.complete();
    }
}