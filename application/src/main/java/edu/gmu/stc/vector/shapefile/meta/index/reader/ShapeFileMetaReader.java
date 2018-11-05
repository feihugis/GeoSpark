/**
 * FILE: ShapeFileReader.java
 * PATH: org.datasyslab.geospark.formatMapper.shapefileParser.shapes.ShapeFileReader.java
 * Copyright (c) 2015-2017 GeoSpark Development Team
 * All rights reserved.
 */
package edu.gmu.stc.vector.shapefile.meta.index.reader;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.datasyslab.geospark.formatMapper.shapefileParser.shapes.ShapeKey;

import java.io.IOException;

import edu.gmu.stc.vector.shapefile.meta.ShapeFileMeta;
import edu.gmu.stc.vector.shapefile.meta.ShpMeta;
import edu.gmu.stc.vector.shapefile.meta.index.parser.ShpFileMetaParser;

public class ShapeFileMetaReader extends RecordReader<ShapeKey, ShpMeta> {

    /** record id */
    private ShapeKey recordKey = null;

    /** primitive bytes value */
    private ShpMeta shpMeta = null;

    /** inputstream for .shp file */
    private FSDataInputStream shpInputStream = null;

    /** file shpParser */
    ShpFileMetaParser shpParser = null;

    /** Iterator of indexes of records */
    private int[] indexes;

    /** whether use index, true when using indexes */
    private boolean useIndex = false;

    /** current index id */
    private int indexId = 0;

    /**
     * empty constructor
     */
    public ShapeFileMetaReader() {
    }

    /**
     * constructor with index
     * @param indexes
     */
    public ShapeFileMetaReader(int[] indexes) {
        this.indexes = indexes;
        useIndex = true;
    }

    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        FileSplit fileSplit = (FileSplit)split;
        Path filePath = fileSplit.getPath();
        FileSystem fileSys = filePath.getFileSystem(context.getConfiguration());
        shpInputStream = fileSys.open(filePath);

        //assign inputstream to shpParser and parse file header to init;
        shpParser = new ShpFileMetaParser(shpInputStream);
        shpParser.parseShapeFileHead();
    }

    public boolean nextKeyValue() throws IOException, InterruptedException {
        if(useIndex){
            /**
             * with index, iterate until end and extract bytes with information from indexes
             */
            if(indexId == indexes.length) return false;
            // check offset, if current offset in inputStream not match with information in shx, move it
            if(shpInputStream.getPos() < indexes[indexId] * 2){
                shpInputStream.skip(indexes[indexId] * 2 - shpInputStream.getPos());
            }
            int currentLength = indexes[indexId + 1] * 2 - 4;
            recordKey = new ShapeKey();
            recordKey.setIndex(shpParser.parseRecordHeadID());
            shpMeta = shpParser.parseRecordPrimitiveContent(currentLength);
            shpMeta.setIndex(recordKey.getIndex());
            indexId += 2;
            return true;
        }else{
            if(getProgress() >= 1) return false;
            recordKey = new ShapeKey();
            recordKey.setIndex(shpParser.parseRecordHeadID());
            shpMeta = shpParser.parseRecordPrimitiveContent();
            return true;
        }
    }

    public ShapeKey getCurrentKey() throws IOException, InterruptedException {
        return recordKey;
    }

    public ShpMeta getCurrentValue() throws IOException, InterruptedException {
        return shpMeta;
    }

    public float getProgress() throws IOException, InterruptedException {
        return shpParser.getProgress();
    }

    public void getBytes(long offset, byte[] holder) throws IOException {
      shpInputStream.readFully(offset, holder);
    }

    public void close() throws IOException {
        shpInputStream.close();
    }
}
