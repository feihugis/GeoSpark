package edu.gmu.stc.config;

/**
 * Created by Fei Hu on 1/29/18.
 */
public class ConfigParameter {

  //Hibernate
  public static final String HIBERNATE_DRIEVER = "hibernate.connection.driver_class";  //org.postgresql.Driver
  public static final String HIBERNATE_URL = "hibernate.connection.url";  //jdbc:postgresql://localhost:5432/hibernate_test
  public static final String HIBERNATE_USER = "hibernate.connection.username";
  public static final String HIBERNATE_PASS = "hibernate.connection.password";
  public static final String HIBERNATE_DIALECT = "hibernate.dialect";  //org.hibernate.dialect.PostgreSQL9Dialect
  public static final String HIBERNATE_HBM2DDL_AUTO = "hibernate.hbm2ddl.auto"; //update

  //HDFS
  public static final String INPUT_DIR_PATH = "mapred.input.dir";
  public static final String SHAPEFILE_INDEX_TABLES = "shapefile.index.tablenames";
  public static final String SHP_LAYER_NAMES = "shapefile.layer.names";

  //Spark
  public static final String EXECUTORS_CORE_NUMBER = "executors.core.number";
  public static final String RDD_SAMPLING_FRACTION = "rdd.sampling.fraction";
  public static final String PARQUET_INDEX_DIRS = "parquet.index.dirs";

  //Geoserver
  public static final String GEOSERVER_HOST_IP = "geoserver.host.ip";
  public static final String GEOSERVER_HOST_USER = "geoserver.host.user";
  public static final String GEOSERVER_HOST_PWD = "geoserver.host.password";
  public static final String GEOSERVER_DATA_DIRECTORY = "geoserver.data.directory";
  public static final String GEOSERVER_RESTURL = "geoserver.resturl";
  public static final String GEOSERVER_RESTUSER= "geoserver.restuser";
  public static final String GEOSERVER_RESTPWD= "geoserver.restpassword";
    public static final String GEOSERVER_WORKSPACE= "geoserver.workspace";
}
