package edu.gmu.stc.vector.shapefile.meta;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.CoordinateFilter;
import com.vividsolutions.jts.geom.CoordinateSequenceComparator;
import com.vividsolutions.jts.geom.CoordinateSequenceFilter;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryComponentFilter;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.GeometryFilter;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.Serializable;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;

/**
 * Created by Fei Hu.
 */
@Entity
public class ShapeFileMeta extends Geometry implements Serializable {
  private static final Log LOG = LogFactory.getLog(ShapeFileMeta.class);

  @Id
  private Long index;
  @Column(name = "typeid")
  private int typeID;
  @Column(name = "shp_offset")
  private long shp_offset;
  @Column(name = "shp_length")
  private int shp_length;
  @Column(name = "dbf_offset")
  private long dbf_offset;
  @Column(name = "dbf_length")
  private int dbf_length;
  @Column(name = "minx")
  private double minX;
  @Column(name = "miny")
  private double minY;
  @Column(name = "maxx")
  private double maxX;
  @Column(name = "maxy")
  private double maxY;
  @Column(name = "filepath")
  private String filePath;

  private Envelope envelope = null;

  public ShapeFileMeta() {
    super(new GeometryFactory());
  }

  public ShapeFileMeta(Long index, int typeID, long shp_offset, int shp_length, long dbf_offset,
                       int dbf_length, String filePath, double minX, double minY, double maxX,
                       double maxY) {
    super(new GeometryFactory());
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
    super(new GeometryFactory());
    this.index = shpMeta.getIndex();
    this.typeID = shpMeta.getTypeID();

    this.shp_offset = shpMeta.getOffset();
    this.shp_length = shpMeta.getLength();

    this.dbf_offset = dbfMeta.getOffset();
    this.dbf_length = dbfMeta.getLength();

    this.filePath = filePath;

    this.minX = shpMeta.getBoundBox().getXMin();
    this.minY = shpMeta.getBoundBox().getYMin();
    this.maxX = shpMeta.getBoundBox().getXMax();
    this.maxY = shpMeta.getBoundBox().getYMax();
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

  public void write(Kryo kryo, Output out) {
    out.writeLong(this.index);
    out.writeInt(this.typeID);
    out.writeLong(this.shp_offset);
    out.writeInt(this.shp_length);
    out.writeLong(this.dbf_offset);
    out.writeInt(this.dbf_length);
    out.writeDouble(this.minX);
    out.writeDouble(this.minY);
    out.writeDouble(this.maxX);
    out.writeDouble(this.maxY);
  }

  public void read(Kryo kryo, Input input) {
    this.index = input.readLong();
    this.typeID = input.readInt();
    this.shp_offset = input.readLong();
    this.shp_length = input.readInt();
    this.dbf_offset = input.readLong();
    this.dbf_length = input.readInt();
    this.minX = input.readDouble();
    this.minY = input.readDouble();
    this.maxX = input.readDouble();
    this.maxY = input.readDouble();
  }

  public String toString() {
    return String.format("Index: %d; TypeID: %d\n"
                         + "\t shp_offset: %d, shp_length: %d; \n "
                         + "\t dbf_offset: %d, dbf_length: %d",
                         index, typeID, shp_offset,
                         shp_length, dbf_offset, dbf_length);
  }

  @Override
  public Geometry reverse() {
    return null;
  }

  @Override
  public boolean equalsExact(Geometry other, double tolerance) {
    return false;
  }

  @Override
  public void apply(CoordinateFilter filter) {

  }

  @Override
  public void apply(CoordinateSequenceFilter filter) {

  }

  @Override
  public void apply(GeometryFilter filter) {

  }

  @Override
  public void apply(GeometryComponentFilter filter) {

  }

  @Override
  public void normalize() {

  }

  @Override
  protected Envelope computeEnvelopeInternal() {
    return null;
  }

  @Override
  protected int compareToSameClass(Object o) {
    return 0;
  }

  @Override
  protected int compareToSameClass(Object o, CoordinateSequenceComparator comp) {
    return 0;
  }

  public static String getSQLForOverlappedRows(String tableName, double minX, double minY, double maxX, double maxY) {

    //TODO: the typeID is hardcoded into 5
    /*String sql = String.format("FROM %s WHERE (%s < minX OR %s > maxX OR %s < minY OR %s > maxY) = FALSE AND typeID = 5",
                               tableName,
                               String.valueOf(maxX), String.valueOf(minX),
                               String.valueOf(maxY), String.valueOf(minY)).toLowerCase();*/
    String sql = String.format("FROM %s", tableName).toLowerCase();
    LOG.info("SQL for querying overlapped rows: " + sql);
    return sql;
  }

  @Override
  public String getGeometryType() {
    return "ShapeFileMeta";
  }

  @Override
  public Coordinate getCoordinate() {
    return new Coordinate();
  }

  @Override
  public Coordinate[] getCoordinates() {
    return new Coordinate[0];
  }

  @Override
  public int getNumPoints() {
    return 4;
  }

  @Override
  public boolean isEmpty() {
    return false;
  }

  @Override
  public int getDimension() {
    return 2;
  }

  @Override
  public Geometry getBoundary() {
    return this.envelope.getOriginalGeometry();
  }

  @Override
  public int getBoundaryDimension() {
    return 2;
  }

  @Override
  public Envelope getEnvelopeInternal() {
    if (this.envelope == null) {
      this.envelope = new Envelope(minX, maxX, minY, maxY);
    }
    return this.envelope;
  }
}
