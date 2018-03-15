import cascading.flow.Flow;
import cascading.flow.FlowDef;
import cascading.flow.local.LocalFlowConnector;
import cascading.operation.Debug;
import cascading.operation.aggregator.Count;
import cascading.operation.regex.RegexFilter;
import cascading.operation.regex.RegexSplitGenerator;
import cascading.pipe.*;
import cascading.pipe.joiner.LeftJoin;
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

        Tap stopWordsTab = new FileTap(new TextDelimited(new Fields("stop")), args[1]);
        Pipe stopWordsPipe = new Pipe("stopwords");

        Pipe joinPipe = new HashJoin(wordsPipe, new Fields("word"), stopWordsPipe, new Fields("stop"), new LeftJoin());

        // remove the records that has stop values not null
        joinPipe = new Each( joinPipe, new Fields("stop"), new RegexFilter( "^$" ) );
        joinPipe = new Each(joinPipe, new Debug("stop words join ---"));


        joinPipe = new GroupBy(joinPipe, new Fields("word"));
        joinPipe = new Every(joinPipe, new Count(), new Fields("word", "count"));

        Tap outPutTap = new FileTap(new TextDelimited(), args[2]);

        FlowDef flowDef = FlowDef.flowDef()
                .addSource(wordsPipe, docTap)
                .addSource(stopWordsPipe, stopWordsTab)
                .addTailSink(joinPipe, outPutTap)
                .setName("words count");

        Flow flow = new LocalFlowConnector().connect(flowDef);
        flow.complete();


    }
}