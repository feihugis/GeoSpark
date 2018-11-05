package edu.gmu.stc.vector.shapefile.reader;

import com.amazonaws.util.json.JSONArray;
import com.amazonaws.util.json.JSONObject;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Polygon;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.datasyslab.geospark.formatMapper.shapefileParser.parseUtils.dbf.DbfParseUtil;
import org.datasyslab.geospark.formatMapper.shapefileParser.shapes.PrimitiveShape;
import org.datasyslab.geospark.formatMapper.shapefileParser.shapes.ShpRecord;
import org.geotools.data.DefaultTransaction;
import org.geotools.data.FeatureWriter;
import org.geotools.data.Transaction;
import org.geotools.data.shapefile.ShapefileDataStore;
import org.geotools.data.shapefile.ShapefileDataStoreFactory;
import org.geotools.data.simple.SimpleFeatureSource;
import org.geotools.data.simple.SimpleFeatureStore;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.geotools.geojson.geom.GeometryJSON;
import org.geotools.referencing.CRS;
import org.geotools.referencing.crs.DefaultGeographicCRS;
import org.geotools.referencing.factory.ReferencingObjectFactory;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.cs.CoordinateSystem;
import org.wololo.geojson.Feature;
import org.wololo.geojson.FeatureCollection;
import org.wololo.jts2geojson.GeoJSONWriter;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
    dbfDataInputStream.read(shapeFileMeta.getDbf_offset(),
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

  public static Geometry readGeometry(byte[] content, int typeid, GeometryFactory geometryFactory)
      throws IOException {
    ShpRecord shpRecord = new ShpRecord(content, typeid);
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

  public static void saveAsShapefile(String filepath, List<Geometry> geometries, String crs)
      throws IOException, FactoryException {
    System.out.println(
            "*****************start writing shapefile ******************");
    Long startTime = System.currentTimeMillis();
    File file = new File(filepath);
    Map<String, Serializable> params = new HashMap<String, Serializable>();
    params.put(ShapefileDataStoreFactory.URLP.key, file.toURI().toURL());
    ShapefileDataStore ds = (ShapefileDataStore) new ShapefileDataStoreFactory().createNewDataStore(params);

    SimpleFeatureTypeBuilder tb = new SimpleFeatureTypeBuilder();

    CoordinateReferenceSystem crsType = CRS.decode(crs);

    tb.setCRS(crsType);

    tb.setName("shapefile");
    tb.add("the_geom", Polygon.class);
    tb.add("outPolyID", Long.class);
    tb.add("minLat", Double.class);
    tb.add("minLon", Double.class);
    tb.add("maxLat", Double.class);
    tb.add("maxLon", Double.class);
    ds.createSchema(tb.buildFeatureType());
    ds.setCharset(Charset.forName("GBK"));

    //ReferencingObjectFactory refFactory = new ReferencingObjectFactory();

    FeatureWriter<SimpleFeatureType, SimpleFeature> writer = ds.getFeatureWriter(ds.getTypeNames()[0], Transaction.AUTO_COMMIT);

    for (int i = 0; i < geometries.size(); i++) {
      SimpleFeature feature = writer.next();
      feature.setAttribute("the_geom", geometries.get(i));
      feature.setAttribute("outPolyID", i);
      Envelope env = geometries.get(i).getEnvelopeInternal();
      feature.setAttribute("minLat", env.getMinY());
      feature.setAttribute("minLon", env.getMinX());
      feature.setAttribute("maxLat", env.getMaxY());
      feature.setAttribute("maxLon", env.getMaxX());
      writer.write();
    }
    writer.close();
    ds.dispose();

    Long endTime = System.currentTimeMillis();
    System.out.println(
            "*****************stop writing shapefile ******************Took " + (endTime - startTime) / 1000 + "s");
  }

  public static void saveAsGeoJSON(String filepath, List<Geometry> geometries, String crs)
      throws IOException {

    System.out.println(
            "*****************start writing jsonfile ******************");
    Long startTime = System.currentTimeMillis();
    GeoJSONWriter geoJSONWriter = new GeoJSONWriter();
    Feature[] features = new Feature[geometries.size()];
    int i = 0;
    for (Geometry geometry : geometries) {
      if (geometry.getUserData() != null) {
        HashMap<String, Object> map = new HashMap<String, Object>();
        map.put("UserData", geometry.getUserData());
        features[i++] = new Feature(geoJSONWriter.write(geometry), map);
      } else {
        features[i++] = new Feature(geoJSONWriter.write(geometry), null);
      }
    }

    FeatureCollection featureCollection = new FeatureCollection(features);

    String geojsonStr = featureCollection.toString();
    FileUtils.writeStringToFile(new File(filepath), geojsonStr);

    Long endTime = System.currentTimeMillis();
    System.out.println(
            "*****************stop writing jsonfile ******************Took " + (endTime - startTime) / 1000 + "s");
  }

  public static String geojson2shp(String jsonFolder, String shpFolder, String crs){

    System.out.println(
            "*****************start convert geojsonfile to shapefile******************");
    Long startTime = System.currentTimeMillis();

    String shpPath = "";
    GeometryJSON gjson = new GeometryJSON();
    try{
      //create shape file
      File shpfolder = new File(shpFolder);
      shpfolder.mkdir();
      shpPath = shpfolder.getPath() + "/" + shpfolder.getName() + ".shp";
      File file = new File(shpPath);

      Map<String, Serializable> params = new HashMap<String, Serializable>();
      params.put( ShapefileDataStoreFactory.URLP.key, file.toURI().toURL() );
      ShapefileDataStore ds = (ShapefileDataStore) new ShapefileDataStoreFactory().createNewDataStore(params);

      //define attribute
      SimpleFeatureTypeBuilder tb = new SimpleFeatureTypeBuilder();
      CoordinateReferenceSystem crsType = CRS.decode(crs);
      tb.setCRS(crsType);

      tb.setName("shapefile");
      tb.add("the_geom", Polygon.class);
      tb.add("POIID", Long.class);
      ds.createSchema(tb.buildFeatureType());
      //set code
      Charset charset = Charset.forName("GBK");
      ds.setCharset(charset);
      //set writer
      FeatureWriter<SimpleFeatureType, SimpleFeature> writer = ds.getFeatureWriter(ds.getTypeNames()[0], Transaction.AUTO_COMMIT);

      File folder = new File(jsonFolder);
      File[] listOfFiles = folder.listFiles();
      int id = 0;
      for (int j = 0; j < listOfFiles.length; j++) {
        if (listOfFiles[j].isFile()) {
          String filePath = listOfFiles[j].getPath();
          String fileName = listOfFiles[j].getName();
          if(fileName.startsWith("_") || fileName.startsWith(".")){
            continue;
          }
          String strJson = new String(Files.readAllBytes(Paths.get(filePath)));
          JSONObject json = new JSONObject(strJson);
          JSONArray features = (JSONArray) json.get("features");
          for (int i = 0, len = features.length(); i < len; i++) {
            String strFeature = features.getString(i);
            Reader reader = new StringReader(strFeature);
            Geometry geometry = gjson.read(reader);
            SimpleFeature feature = writer.next();
            feature.setAttribute("the_geom", geometry);
            feature.setAttribute("POIID", id);
            id += 1;
            writer.write();
          }
        }
      }

      System.out.println( id + " polygons");
      writer.close();
      ds.dispose();
    }
    catch(Exception e){
      e.printStackTrace();
    }

    Long endTime = System.currentTimeMillis();
    System.out.println(
            "*****************stop converting geojsonfile to shapefile ******************Took " + (endTime - startTime) / 1000 + "s");

    return shpPath;
  }

  public static void main(String[] args) {
    String localGeoJsonFolder = "/Users/feihu/Desktop/GeoJson";
    String shpFolder = "/Users/feihu/Desktop/shp";
    String crs = "epsg:4326";
    GeometryReaderUtil.geojson2shp(localGeoJsonFolder, shpFolder, crs);

  }
}


