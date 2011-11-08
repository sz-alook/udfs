package com.kat.udf;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.apache.pig.LoadFunc;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigFileInputFormat;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigTextInputFormat;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;

public class PigJsonLoader extends LoadFunc {
    private static final Logger LOG = LoggerFactory.getLogger(PigJsonLoader.class);
    private static final TupleFactory tupleFactory_ = TupleFactory.getInstance();
    private final JSONParser jsonParser_ = new JSONParser();
    private LineRecordReader in = null;

    public PigJsonLoader() {

    }

    @SuppressWarnings("unchecked")
    @Override
    public InputFormat getInputFormat() throws IOException {
        return new PigTextInputFormat();
    }

    @Override
    public Tuple getNext() throws IOException {
        boolean notDone = in.nextKeyValue();
        if (!notDone) {
            return null;
        }

        String line;
        Text val = in.getCurrentValue();
        if (val == null) {
            return null;
        }

        line = val.toString();
        if (line.length() > 0) {
            Tuple t = parseStringToTuple(line);

            if (t != null) {
                return t;
            }
        }

        return null;
    }

    protected Tuple parseStringToTuple(String line) {
        try {
            Map<String, String> values = Maps.newHashMap();
            JSONObject jsonObj = (JSONObject) jsonParser_.parse(line);
            for (Object key : jsonObj.keySet()) {
                Object value = jsonObj.get(key);
                values.put(key.toString(), value != null ? value.toString()
                        : null);
            }
            return tupleFactory_.newTuple(values);
        } catch (ParseException e) {
            LOG.warn("Could not json-decode string: " + line, e);
            return null;
        } catch (NumberFormatException e) {
            LOG.warn("Very big number exceeds the scale of long: " + line, e);
            return null;
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public void prepareToRead(RecordReader reader, PigSplit split)
            throws IOException {
        in = (LineRecordReader) reader;
    }

    @Override
    public void setLocation(String location, Job job) throws IOException {
        PigFileInputFormat.setInputPaths(job, location);
    }
}
