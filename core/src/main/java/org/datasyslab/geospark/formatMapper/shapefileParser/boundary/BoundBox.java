/**
 * FILE: BoundBox.java
 * PATH: org.datasyslab.geospark.formatMapper.shapefileParser.boundary.BoundBox.java
 * Copyright (c) 2015-2017 GeoSpark Development Team
 * All rights reserved.
 */
package org.datasyslab.geospark.formatMapper.shapefileParser.boundary;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.Polygon;

import java.io.Serializable;
import java.util.Arrays;

/**
 * Created by zongsizhang on 7/6/17.
 */
public class BoundBox implements Serializable{

    /** bounds of 8 numbers. Xmin, Ymin, Xmax, Ymax, Zmin, Zmax, Mmin, Mmax */
    double[] bounds = null;

    /**
     * construct bounds with an array
     * @param bounds
     */
    public BoundBox(double[] bounds) {
        this.bounds = Arrays.copyOf(bounds, bounds.length);
    }

    /**
     * construct by copy other boundbox
     * @param otherbox
     */
    public BoundBox(BoundBox otherbox) {
        this.bounds = otherbox.copyBounds();
    }

    /**
     * construct a initial boundBox with all value 0
     */
    public BoundBox() {
        bounds = new double[8];
    }

    /**
     * set tuple at i with value
     * @param i
     * @param value
     */
    public void set(int i, double value){
        bounds[i] = value;
    }

    /**
     * return a copy of bounds
     * @return
     */
    public double[] copyBounds(){
        return Arrays.copyOf(bounds, bounds.length);
    }

    /**
     * convert bounds array to string
     * @return
     */
    @Override
    public String toString() {
        StringBuilder strBuilder = new StringBuilder();
        for (int i = 0;i < bounds.length; ++i){
            strBuilder.append(bounds[i] + ", ");
        }
        return strBuilder.toString();
    }

    /** set min X */
    public void setXMin(double value){
        bounds[0] = value;
    }

    /** set min Y */
    public void setYMin(double value){
        bounds[1] = value;
    }

    /** set max X */
    public void setXMax(double value){
        bounds[2] = value;
    }

    /** set max Y */
    public void setYMax(double value){
        bounds[3] = value;
    }

    /** set min Z */
    public void setZMin(double value){
        bounds[4] = value;
    }

    /** set max Z */
    public void setZMax(double value){
        bounds[5] = value;
    }

    /** set min M */
    public void setMMin(double value){
        bounds[6] = value;
    }

    /** set max M */
    public void setMMax(double value){
        bounds[7] = value;
    }

    /**get min X */
    public double getXMin(){
        return bounds[0];
    }

    /**get max X */
    public double getXMax(){
        return bounds[2];
    }

    /**get min Y */
    public double getYMin(){
        return bounds[1];
    }

    /**get max Y */
    public double getYMax(){
        return bounds[3];
    }

    /**get min Z */
    public double getZMin(){
        return bounds[4];
    }

    /**get max Z */
    public double getZMax(){
        return bounds[5];
    }

    /**get min M */
    public double getMMin(){
        return bounds[6];
    }

    /**get max M */
    public double getMMax(){
        return bounds[7];
    }

    /**
     * calculate the union of two bound box
     * @param box1
     * @param box2
     * @return
     */
    public static BoundBox mergeBoundBox(BoundBox box1, BoundBox box2){
        BoundBox box = new BoundBox();
        // merge X
        box.setXMin(Math.min(box1.getXMin(), box2.getXMin()));
        box.setXMax(Math.max(box1.getXMax(), box2.getXMax()));
        // merge Y
        box.setYMin(Math.min(box1.getYMin(), box2.getYMin()));
        box.setYMax(Math.max(box1.getYMax(), box2.getYMax()));
        // merge Z
        box.setZMin(Math.min(box1.getZMin(), box2.getZMin()));
        box.setZMax(Math.max(box1.getZMax(), box2.getZMax()));
        // merge M
        box.setMMin(Math.min(box1.getMMin(), box2.getMMin()));
        box.setMMax(Math.max(box1.getMMax(), box2.getMMax()));
        return box;
    }
}
