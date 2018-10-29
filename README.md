# Spark-based Framework for Big GeoSpatial Vector Data Analytics

This repository is developping a Spark-based framework for big geospatial vector data analytics
using GeoSpark.

## Example
 * edu.gmu.stc.vector.examples.GeoSparkExample


## Notes
 * Maven pom file dependence: the difference between `provider` and `compile`(default) of the scope value
 * Compile jars: `mvn clean install -DskipTests=true`
 * Connect PostgreSQL: ` psql -h 10.192.21.133 -p 5432 -U postgres -W`
 * Run Spark-shell: `spark-shell --master yarn --deploy-mode client 
   --jars /root/geospark/geospark-application-1.1.0-SNAPSHOT.jar --num-executors 5 --driver-memory 12g 
   --executor-memory 10g --executor-cores 22`
 * Submit Spark-jobs: 
    - Available GridType: `EQUALGRID`, `HILBERT`, `RTREE`, `VORONOI`, `QUADTREE`, `KDBTREE`
    - Available IndexType: `RTREE`, `QUADTREE`
    - GeoSpark: `spark-submit --master yarn --deploy-mode client --num-executors 5 --driver-memory 12g --executor-memory 10g --executor-cores 22 --class edu.gmu.stc.vector.sparkshell.OverlapPerformanceTest /root/geospark/geospark-application-1.1.0-SNAPSHOT.jar /user/root/data0119/ Impervious_Surface_2015_DC Soil_Type_by_Slope_DC Partition_Num GridType IndexType`
    - STCSpark_Build_Index: `spark-shell --master yarn --deploy-mode client --num-executors 5 --driver-memory 12g --executor-memory 10g --executor-cores 24 --class edu.gmu.stc.vector.sparkshell.STC_BuildIndexTest --jars /home/fei/GeoSpark/application/target/geospark-application-1.1.0-SNAPSHOT.jar BuildIndex /home/fei/GeoSpark/config/conf.xml`
    - STCSpark_Overlap_v1: `spark-shell --master yarn --deploy-mode client --num-executors 5 --driver-memory 12g --executor-memory 10g --executor-cores 24 --class edu.gmu.stc.vector.sparkshell.STC_OverlapTest --jars /home/fei/GeoSpark/application/target/geospark-application-1.1.0-SNAPSHOT.jar /home/fei/GeoSpark/config/conf.xml Partition_Num GridType IndexType /test.geojson`
    - STCSpark_Overlap_v2: `spark-shell --master yarn --deploy-mode client --num-executors 5 --driver-memory 12g --executor-memory 10g --executor-cores 24 --class edu.gmu.stc.vector.sparkshell.STC_OverlapTest_v2 --jars /home/fei/GeoSpark/application/target/geospark-application-1.1.0-SNAPSHOT.jar /home/fei/GeoSpark/config/conf.xml Partition_Num GridType IndexType /test.geojson`
    - STCSpark_Overlap_v3: `spark-shell --master yarn --deploy-mode client --num-executors 5 --driver-memory 12g --executor-memory 10g --executor-cores 24 --class edu.gmu.stc.vector.sparkshell.STC_OverlapTest_v3 --jars /home/fei/GeoSpark/application/target/geospark-application-1.1.0-SNAPSHOT.jar /home/fei/GeoSpark/config/conf.xml Partition_Num GridType IndexType /test.geojson`
 
 * Multiple-layer overlay
    - Build index: `~/spark-2.3.0-bin-hadoop2.6/bin//spark-shell --master yarn --deploy-mode client --num-executors 10 --driver-memory 12g --executor-memory 10g --executor-cores 1 --jars application/target/geospark-application-1.1.0-SNAPSHOT.jar BuildIndex config/conf_osm.xml`
    - Config xml files need to be uploaded into HDFS, and the xml file is only for the database configuration
    - Prepare for the Hadoop environment: `export HADOOP_CONF_DIR=/opt/cloudera/parcels/CDH-5.5.1-1.cdh5.5.1.p0.11/lib/spark/conf/yarn-conf/`
    - Multiple-layer overlay: 
        - Client mode `~/spark-2.3.0-bin-hadoop2.6/bin/spark-shell --master yarn --deploy-mode client --num-executors 10 --driver-memory 12g --executor-memory 10g --executor-cores 12 --class edu.gmu.stc.vector.sparkshell.Multilayer_Overlap_v1 --jars application/target/geospark-application-1.1.0-SNAPSHOT.jar /osm/config/conf_osm.xml 30 KDBTREE QUADTREE /multiOverlay_test/ epsg:4326 va_gis_osm_landuse_a_free_1 /osm/virginia-latest-free`
        - Yarn mode `~/spark-2.3.0-bin-hadoop2.6/bin/spark-submit --master yarn --deploy-mode cluster --num-executors 10 --driver-memory 12g --executor-memory 10g --executor-cores 12 --class edu.gmu.stc.vector.sparkshell.Multilayer_Overlap_v1 application/target/geospark-application-1.1.0-SNAPSHOT.jar /osm/config/conf_osm.xml 30 KDBTREE QUADTREE /multiOverlay_test/ epsg:4326 va_gis_osm_landuse_a_free_1 /osm/virginia-latest-free` 
 
 ---
 ### Parquet
 1. Building index:
 ```
 $ /opt/cloudera/parcels/SPARK2-2.1.0.cloudera1-1.cdh5.7.0.p0.120904/lib/spark2/bin/spark-shell \
 --master yarn \
 --deploy-mode client \
 --num-executors 5 \
 --driver-memory 12g \
 --executor-memory 10g \
 --executor-cores 24 \
 --class edu.gmu.stc.vector.sparkshell.STC_BuildIndexTest \
 --jars /home/fei/GeoSpark/application/target/geospark-application-1.1.0-SNAPSHOT.jar \
 BuildIndex \
 /home/fei/GeoSpark/config/conf_ma_cluster.xml
 ```
 
 2. Run STCSPark overlap test (Note: the config xml file need to be put on HDFS):
 ```
 $ /opt/cloudera/parcels/SPARK2-2.1.0.cloudera1-1.cdh5.7.0.p0.120904/lib/spark2/bin/spark-shell \
 --master yarn \
 --deploy-mode client \
 --num-executors 5 \
 --driver-memory 12g \
 --executor-memory 10g \
 --executor-cores 24 \
 --class edu.gmu.stc.vector.sparkshell.STC_OverlapTest_v4 \
 --jars /home/fei/GeoSpark/application/target/geospark-application-1.1.0-SNAPSHOT.jar \
  /user/root/geospark/config/conf_ma_cluster.xml \
  120 \
  KDBTREE \
  QUADTREE \
  /user/root/test/test.geojson \
  epsg:4326
 ```
 
 3. Run GeoSpark Overlap:
 ```
 $ /opt/cloudera/parcels/SPARK2-2.1.0.cloudera1-1.cdh5.7.0.p0.120904/lib/spark2/bin/spark-shell \
 --master yarn \
 --deploy-mode client \
 --num-executors 5 \
 --driver-memory 12g \
 --executor-memory 10g \
 --executor-cores 24 \
 --class edu.gmu.stc.vector.sparkshell.GeoSpark_OverlapTest \
 --jars /home/fei/GeoSpark/application/target/geospark-application-1.1.0-SNAPSHOT.jar \
 /user/root/MA_test/ \
 MA_gis_osm_buildings \
 MA_gis_osm_landuse \
 120 \
 KDBTREE \
 QUADTREE
 ```