/**
 * FILE: DbfFileReader.java
 * PATH: org.datasyslab.geospark.formatMapper.shapefileParser.shapes.DbfFileReader.java
 * Copyright (c) 2015-2017 GeoSpark Development Team
 * All rights reserved.
 */
package edu.gmu.stc.vector.shapefile.meta.index.reader;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.datasyslab.geospark.formatMapper.shapefileParser.parseUtils.dbf.DbfParseUtil;
import org.datasyslab.geospark.formatMapper.shapefileParser.shapes.ShapeKey;

import java.io.IOException;

import edu.gmu.stc.vector.shapefile.meta.DbfMeta;
import edu.gmu.stc.vector.shapefile.meta.index.parser.DbfMetaParser;

public class DbfFileMetaReader extends org.apache.hadoop.mapreduce.RecordReader<ShapeKey, DbfMeta> {

    /** inputstream of .dbf file */
    private FSDataInputStream inputStream = null;

    /** primitive bytes array of one row */
    private DbfMeta value = null;

    /** key value of current row */
    private ShapeKey key = null;

    /** generated id of current row */
    private int id = 0;

    /** Dbf parser */
    DbfMetaParser dbfParser = null;

    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        FileSplit fileSplit = (FileSplit)split;
        Path inputPath = fileSplit.getPath();
        FileSystem fileSys = inputPath.getFileSystem(context.getConfiguration());
        inputStream = fileSys.open(inputPath);
        dbfParser = new DbfMetaParser();
        dbfParser.parseFileHead(inputStream);
    }

    public boolean nextKeyValue() throws IOException, InterruptedException {
        // first check deleted flag
        DbfMeta curbytes = dbfParser.parsePrimitiveRecord(inputStream);
        if(curbytes == null){
            value = null;
            return false;
        }
        else{
            value = curbytes;
            key = new ShapeKey();
            key.setIndex(id++);
            return true;
        }
    }

    public ShapeKey getCurrentKey() throws IOException, InterruptedException {
        return key;
    }

    public DbfMeta getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    public float getProgress() throws IOException, InterruptedException {
        return dbfParser.getProgress();
    }

    public void close() throws IOException {
        inputStream.close();
    }
}
