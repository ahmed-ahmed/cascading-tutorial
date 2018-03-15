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
        Tap docTap = new FileTap(new TextDelimited(), args[0]);

        Pipe wordsPipe = new Each("wordsPipe",  new RegexSplitGenerator(new Fields("word"), "[ \\\\[\\\\]\\\\(\\\\),.]"), new Fields("word"));

        wordsPipe = new Each(wordsPipe, new Fields("word"), new ScrubFunction(new Fields("word")));

        wordsPipe = new Each(wordsPipe, new Debug("words"));

        wordsPipe = new GroupBy(wordsPipe, new Fields("word"));
        wordsPipe = new Every(wordsPipe, new Count(), new Fields("word", "count"));

        Tap outPutTap = new FileTap(new TextDelimited(), args[1]);

        FlowDef flowDef = FlowDef.flowDef()
                .addSource(wordsPipe, docTap)
                .addTailSink(wordsPipe, outPutTap)
                .setName("words count");

        Flow flow = new LocalFlowConnector().connect(flowDef);
        flow.complete();


    }
}