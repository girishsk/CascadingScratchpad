package wordcount;

import java.util.Properties;

import cascading.flow.Flow;
import cascading.flow.FlowDef;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.pipe.Pipe;
import cascading.pipe.Each;
import cascading.property.AppProps;
import cascading.scheme.hadoop.TextLine;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;
import cascading.operation.regex.RegexSplitGenerator;

public class
  Main
  {
  public static void
  main( String[] args )
    {
    String in = args[ 0 ];
    String out = args[ 1 ];

    Properties properties = new Properties();
    AppProps.setApplicationJarClass( properties, Main.class );
    HadoopFlowConnector flowConnector = new HadoopFlowConnector( properties );

    Fields line = new Fields("line");
    Fields count = new Fields("count");
    
    Fields fieldDec = new Fields("count");
    
    
    // source tap
    Tap source = new Hfs( new TextLine(line) ,in );

    // sink tap
    Tap sink = new Hfs( new TextLine( count) , out );

    RegexSplitGenerator regex = new RegexSplitGenerator( count, "[ \\[\\]\\(\\),.]" );

    Pipe pipe = new Pipe( "wcpipe" );
   
    pipe = new Each("count", line, regex,fieldDec);

    FlowDef flow = FlowDef.flowDef()
	.setName("wc-flow")
        .addSource( pipe, source )
        .addTailSink( pipe, sink );

    // run the flow
    flowConnector.connect( flow ).complete();
    }
  }
