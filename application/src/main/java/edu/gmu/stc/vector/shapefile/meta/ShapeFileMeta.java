package edu.gmu.stc.vector.shapefile.meta;

import java.io.Serializable;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;

/**
 * Created by Fei Hu.
 */
@Entity
public class ShapeFileMeta implements Serializable {
  @Id
  private Long index;
  @Column(name = "typeID")
  private int typeID;
  @Column(name = "shp_offset")
  private long shp_offset;
  @Column(name = "shp_length")
  private int shp_length;
  @Column(name = "dbf_offset")
  private long dbf_offset;
  @Column(name = "dbf_length")
  private int dbf_length;
  @Column(name = "filePath")
  private String filePath;
  @Column(name = "minX")
  private double minX;
  @Column(name = "minY")
  private double minY;
  @Column(name = "maxX")
  private double maxX;
  @Column(name = "maxY")
  private double maxY;

  public ShapeFileMeta(Long index, int typeID, long shp_offset, int shp_length, long dbf_offset,
                       int dbf_length, String filePath, double minX, double minY, double maxX,
                       double maxY) {
    this.index = index;
    this.typeID = typeID;
    this.shp_offset = shp_offset;
    this.shp_length = shp_length;
    this.dbf_offset = dbf_offset;
    this.dbf_length = dbf_length;
    this.filePath = filePath;
    this.minX = minX;
    this.minY = minY;
    this.maxX = maxX;
    this.maxY = maxY;
  }

  public ShapeFileMeta(ShpMeta shpMeta, DbfMeta dbfMeta, String filePath) {
    this.index = shpMeta.getIndex();
    this.typeID = shpMeta.getTypeID();

    this.shp_offset = shpMeta.getOffset();
    this.shp_length = shpMeta.getLength();

    this.dbf_offset = dbfMeta.getOffset();
    this.dbf_length = dbfMeta.getLength();

    this.filePath = filePath;
  }

  public Long getIndex() {
    return index;
  }

  public void setIndex(Long index) {
    this.index = index;
  }

  public int getTypeID() {
    return typeID;
  }

  public void setTypeID(int typeID) {
    this.typeID = typeID;
  }

  public long getShp_offset() {
    return shp_offset;
  }

  public void setShp_offset(long shp_offset) {
    this.shp_offset = shp_offset;
  }

  public int getShp_length() {
    return shp_length;
  }

  public void setShp_length(int shp_length) {
    this.shp_length = shp_length;
  }

  public long getDbf_offset() {
    return dbf_offset;
  }

  public void setDbf_offset(long dbf_offset) {
    this.dbf_offset = dbf_offset;
  }

  public int getDbf_length() {
    return dbf_length;
  }

  public void setDbf_length(int dbf_length) {
    this.dbf_length = dbf_length;
  }

  public String getFilePath() {
    return filePath;
  }

  public void setFilePath(String filePath) {
    this.filePath = filePath;
  }

  public double getMinX() {
    return minX;
  }

  public void setMinX(double minX) {
    this.minX = minX;
  }

  public double getMinY() {
    return minY;
  }

  public void setMinY(double minY) {
    this.minY = minY;
  }

  public double getMaxX() {
    return maxX;
  }

  public void setMaxX(double maxX) {
    this.maxX = maxX;
  }

  public double getMaxY() {
    return maxY;
  }

  public void setMaxY(double maxY) {
    this.maxY = maxY;
  }

  public String toString() {
    return String.format("Index: %d; TypeID: %d\n"
                         + "\t shp_offset: %d, shp_length: %d; \n "
                         + "\t dbf_offset: %d, dbf_length: %d",
                         index, typeID, shp_offset,
                         shp_length, dbf_offset, dbf_length);
  }
}
