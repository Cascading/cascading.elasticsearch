package backtype.hadoop;

/**
 * Created by IntelliJ IDEA.
 * User: sritchie
 * Date: 11/15/11
 * Time: 12:23 AM
 * To change this template use File | Settings | File Templates.
 */

import java.beans.ConstructorProperties;
import java.io.IOException;
import java.util.HashMap;

import com.infochimps.elasticsearch.ElasticSearchInputFormat;
import com.infochimps.elasticsearch.ElasticSearchOutputFormat;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.InputFormat;
import org.codehaus.jackson.map.ObjectMapper;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.log4j.Logger;

import cascading.scheme.Scheme;
import cascading.tap.Tap;
import cascading.tap.TapException;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

import backtype.hadoop.ElasticUtil;
import org.codehaus.jackson.map.JsonMappingException;
import org.elasticsearch.common.jackson.JsonParseException;

public class ElasticScheme extends Scheme {

    /** Field serialVersionUID */
    private static final long serialVersionUID = 1L;

    /** Field DEFAULT_FIELDS */
    public static final Fields DEFAULT_FIELDS = new Fields( "json" );
    private static final Logger LOGGER = Logger.getLogger(ElasticScheme.class);
    protected ObjectMapper mapper = new ObjectMapper();
    
    public ElasticScheme() {
        super( DEFAULT_FIELDS );
    }

    /**
   * Creates a new TextLine instance that sources "offset" and "line" fields, and sinks all incoming fields, where
   * "offset" is the byte offset in the input file.
   *
   * @param numSinkParts of type int
   */
    @ConstructorProperties({"numSinkParts"})
    public ElasticScheme( int numSinkParts ) {
        super( DEFAULT_FIELDS, numSinkParts );
    }

    @ConstructorProperties({"field"})
    public ElasticScheme( Fields field ) {
        super(field, field);

        if( field.size() != 1 )
            throw new IllegalArgumentException( "this scheme requires a single field, not [" + field + "]" );
    }

    @ConstructorProperties({"field", "numSinkParts"})
    public ElasticScheme( Fields field, int numSinkParts )
    {
        super( field, numSinkParts );

        if( field.size() != 1)
            throw new IllegalArgumentException( "this scheme requires a single field, not [" + getSourceFields() + "]" );
    }

    
    
    @Override
    public void sourceInit(Tap tap, JobConf jobConf) throws IOException {
        jobConf.setInputFormat((Class<? extends InputFormat>) ElasticSearchInputFormat.class);
        LOGGER.info(String.format("Initializing ElasticSearch source tap - field: %s", getSourceFields()));
    }

    @Override public void sinkInit(Tap tap, JobConf jobConf) throws IOException {
        jobConf.setOutputKeyClass( NullWritable.class ); // be explicit
        jobConf.setOutputValueClass( MapWritable.class ); // be explicit
        jobConf.setOutputFormat((Class<? extends OutputFormat>) ElasticSearchOutputFormat.class);

        LOGGER.info(String.format("Initializing ElasticSearch sink tap - field: %s", getSinkFields()));
    }
    
    @Override
    public Tuple source( Object key, Object value )
    {
        Tuple tuple = Tuple.size(2);

        // TODO: Check that tuple value is json!
        tuple.add( key.toString() );   // docID
        tuple.add( value.toString() ); // docContent
        return tuple;
    }

    @Override
    public void sink(TupleEntry tupleEntry, OutputCollector outputCollector) throws IOException {
        MapWritable record  = new MapWritable();
        if (tupleEntry != null ) {
            String jsonData = tupleEntry.get(0).toString();

            // parse json data and put into mapwritable record
            try {
                HashMap<String,Object> data = mapper.readValue(jsonData, HashMap.class);
                record = (MapWritable) ElasticUtil.toWritable(data);
            } catch (JsonParseException e) {
                e.printStackTrace();
            } catch (JsonMappingException e) {
                e.printStackTrace();
            }
        }

        outputCollector.collect(NullWritable.get(), record);
    }
}
