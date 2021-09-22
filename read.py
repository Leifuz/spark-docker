import sys, os
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession, Catalog
from pyspark.sql import DataFrame, DataFrameStatFunctions, DataFrameNaFunctions
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.types import Row

from sedona.register import SedonaRegistrator
from sedona.utils import SedonaKryoRegistrator, KryoSerializer

from sedona.utils.adapter import Adapter

from sedona.core.geom.envelope import Envelope
from sedona.core.spatialOperator import RangeQuery
from sedona.core.enums import IndexType

spark_conf = SparkConf()
spark_conf.setAll([
    ('spark.master', 'spark://172.21.0.3:7077'),
    ('spark.app.name', 'spark-docker-snippet-sedona'),
    ('spark.executor.memory', '4g'),
    ('spark.executor.cores', '2'),
    ('spark.submit.deployMode', 'client'),
    ('spark.ui.showConsoleProgress', 'true'),
    ('spark.eventLog.enabled', 'false'),
    ('spark.logConf', 'false'),
    ('spark.driver.bindAddress', '0.0.0.0'),
    ('spark.driver.host', '172.21.0.7'),
    ('spark.serializer', KryoSerializer.getName),
    ('spark.kryo.registrator', SedonaKryoRegistrator.getName),
    ('spark.jars.packages','org.apache.sedona:sedona-python-adapter-3.0_2.12:1.0.1-incubating,org.datasyslab:geotools-wrapper:geotools-24.1')
])

spark_sess = SparkSession.builder.config(conf=spark_conf).getOrCreate()
spark_ctxt = spark_sess.sparkContext
spark_reader = spark_sess.read
spark_streamReader = spark_sess.readStream
spark_ctxt.setLogLevel("WARN")
spark_ctxt.setSystemProperty("sedona.global.charset", "utf-8")

SedonaRegistrator.registerAll(spark_sess)

"""from sedona.core.formatMapper.shapefileParser import ShapefileReader

ShapefileReader.readToGeometryRDD(spark_ctxt, "/opt/bitnami/spark/data/Unidades_Administrativas/Unidades_Administrativas.shp")
"""

"""from sedona.core.formatMapper.shapefileParser import ShapefileReader

ShapefileReader.readToGeometryRDD(spark_ctxt, "/opt/bitnami/spark/data/Limite_RAs/Unidades_Administrativas.shp")
"""
from sedona.core.formatMapper import GeoJsonReader

ras_rdd = GeoJsonReader.readToGeometryRDD(spark_ctxt, "/opt/bitnami/spark/data/Limite_RAs.geojson")
"""

from sedona.core.formatMapper.shapefileParser import ShapefileReader

ShapefileReader.readToGeometryRDD(spark_ctxt, "/opt/bitnami/spark/data/random_poly_test.shp")
"""