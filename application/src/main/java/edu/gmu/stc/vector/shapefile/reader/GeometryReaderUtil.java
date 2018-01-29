package edu.gmu.stc.vector.shapefile.reader;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.datasyslab.geospark.formatMapper.shapefileParser.parseUtils.dbf.DbfParseUtil;
import org.datasyslab.geospark.formatMapper.shapefileParser.shapes.PrimitiveShape;
import org.datasyslab.geospark.formatMapper.shapefileParser.shapes.ShpRecord;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import edu.gmu.stc.vector.shapefile.meta.ShapeFileMeta;

/**
 * Created by Fei Hu on 1/26/18.
 */
public class GeometryReaderUtil {
  /** suffix of attribute file */
  public final static String DBF_SUFFIX = ".dbf";

  /** suffix of shape record file */
  public final static String SHP_SUFFIX = ".shp";

  /** suffix of index file */
  public final static String SHX_SUFFIX = ".shx";


  public static Geometry readGeometryWithAttributes(FSDataInputStream shpDataInputStream,
                                                    FSDataInputStream dbfDataInputStream,
                                                    ShapeFileMeta shapeFileMeta,
                                                    DbfParseUtil dbfParseUtil,
                                                    GeometryFactory geometryFactory) throws IOException {
    byte[] content = new byte[shapeFileMeta.getShp_length()];
    shpDataInputStream.read(shapeFileMeta.getShp_offset(),
                            content,
                            0,
                            shapeFileMeta.getShp_length());
    ShpRecord shpRecord = new ShpRecord(content, shapeFileMeta.getTypeID());


    PrimitiveShape value = new PrimitiveShape(shpRecord);

    byte[] primitiveBytes = new byte[shapeFileMeta.getDbf_length()];
    dbfDataInputStream.read(shapeFileMeta.getShp_offset(),
                            primitiveBytes,
                            0,
                            shapeFileMeta.getDbf_length());
    String attributes = dbfParseUtil.primitiveToAttributes(ByteBuffer.wrap(primitiveBytes));
    value.setAttributes(attributes);

    return value.getShape(geometryFactory);
  }

  public static Geometry readGeometry(FSDataInputStream shpDataInputStream,
                                      ShapeFileMeta shapeFileMeta,
                                      GeometryFactory geometryFactory) throws IOException {
    byte[] content = new byte[shapeFileMeta.getShp_length()];
    shpDataInputStream.read(shapeFileMeta.getShp_offset(), content,
                            0, shapeFileMeta.getShp_length());
    ShpRecord shpRecord = new ShpRecord(content, shapeFileMeta.getTypeID());
    PrimitiveShape value = new PrimitiveShape(shpRecord);
    return value.getShape(geometryFactory);
  }

  public static List<Geometry> readGeometriesWithAttributes(List<ShapeFileMeta> shapeFileMetaList)
      throws IOException {
    List<Geometry> geometries = new ArrayList<Geometry>();
    if (shapeFileMetaList.isEmpty()) return geometries;

    Path shpFilePath = new Path(shapeFileMetaList.get(0).getFilePath()
                                + GeometryReaderUtil.SHP_SUFFIX);
    Path dbfFilePath = new Path(shapeFileMetaList.get(0).getFilePath()
                                + GeometryReaderUtil.DBF_SUFFIX);
    FileSystem fs = shpFilePath.getFileSystem(new Configuration());
    FSDataInputStream shpInputStream = fs.open(shpFilePath);
    FSDataInputStream dbfInputStream = fs.open(dbfFilePath);
    DbfParseUtil dbfParseUtil = new DbfParseUtil();
    dbfParseUtil.parseFileHead(dbfInputStream);
    GeometryFactory geometryFactory = new GeometryFactory();

    for (ShapeFileMeta shapeFileMeta : shapeFileMetaList) {
      geometries.add(GeometryReaderUtil.readGeometryWithAttributes(shpInputStream, dbfInputStream,
                                                                   shapeFileMeta, dbfParseUtil,
                                                                   geometryFactory));
    }

    return geometries;
  }

  public static List<Geometry> readGeometries(List<ShapeFileMeta> shapeFileMetaList)
      throws IOException {
    List<Geometry> geometries = new ArrayList<Geometry>();
    if (shapeFileMetaList.isEmpty()) return geometries;

    Path shpFilePath = new Path(shapeFileMetaList.get(0).getFilePath()
                                + GeometryReaderUtil.SHP_SUFFIX);

    FileSystem fs = shpFilePath.getFileSystem(new Configuration());
    FSDataInputStream shpInputStream = fs.open(shpFilePath);
    GeometryFactory geometryFactory = new GeometryFactory();

    for (ShapeFileMeta shapeFileMeta : shapeFileMetaList) {
      geometries.add(GeometryReaderUtil.readGeometry(shpInputStream, shapeFileMeta, geometryFactory));
    }

    return geometries;
  }



}