/**
 * FILE: ShpRecord.java
 * PATH: org.datasyslab.geospark.formatMapper.shapefileParser.shapes.ShpRecord.java
 * Copyright (c) 2015-2017 GeoSpark Development Team
 * All rights reserved.
 */
package org.datasyslab.geospark.formatMapper.shapefileParser.shapes;

import org.apache.hadoop.io.BytesWritable;
import org.datasyslab.geospark.formatMapper.shapefileParser.parseUtils.shp.ShapeType;

import java.io.Serializable;

public class ShpRecord implements Serializable {

    /** primitive byte contents */
    private BytesWritable bytes = null;

    /** shape type */
    private int typeID = -1;

    /**
     * create a ShpRecord with primitive bytes and shape type id we abstract from .shp file
     * @param byteArray
     * @param shapeTypeID
     */
    public ShpRecord(byte[] byteArray, int shapeTypeID) {
        bytes = new BytesWritable();
        bytes.set(byteArray, 0, byteArray.length);
        typeID = shapeTypeID;
    }

  public ShpRecord(Object byteArray, int shapeTypeID) {
    bytes = new BytesWritable();
    byte[] byteArray_input = (byte[]) byteArray;
    bytes.set(byteArray_input, 0, byteArray_input.length);
    typeID = shapeTypeID;
  }

    public BytesWritable getBytes() {
        return bytes;
    }

    public int getTypeID() {
        return typeID;
    }
}
