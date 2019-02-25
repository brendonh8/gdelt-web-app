import sys.process._
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType,  DoubleType}
import org.apache.spark.sql.SaveMode
import sqlContext.implicits._
import java.util.Calendar
import java.io.IOException

import com.github.davidmoten.geo._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.IOUtils

import scala.util.Try


def extract_time_series(filename: String) : org.apache.spark.sql.DataFrame = {

	// filename will be the raw gkg file located in the s3 bucket. Column 18
	// contains the sentiment GCAM info to be extracted.

	val gkg_core_schema = StructType(Array(
        StructField("GkgRecordId"           , StringType, true), //$1       
        StructField("V21Date"               , StringType, true), //$2       
        StructField("V2SrcCollectionId"     , StringType, true), //$3       
        StructField("V2SrcCmnName"          , StringType, true), //$4
        StructField("V2DocId"               , StringType, true), //$5
        StructField("V1Counts"              , StringType, true), //$6
        StructField("V21Counts"             , StringType, true), //$7
        StructField("V1Themes"              , StringType, true), //$8
        StructField("V2Themes"              , StringType, true), //$9
        StructField("V1Locations"           , StringType, true), //$10
        StructField("V2Locations"           , StringType, true), //$11
        StructField("V1Persons"             , StringType, true), //$12
        StructField("V2Persons"             , StringType, true), //$13    
        StructField("V1Orgs"                , StringType, true), //$14
        StructField("V2Orgs"                , StringType, true), //$15
        StructField("V15Tone"               , StringType, true), //$16
        StructField("V21Dates"              , StringType, true), //$17
        StructField("V2GCAM"                , StringType, true), //$18
        StructField("V21ShareImg"           , StringType, true), //$19
        StructField("V21RelImg"             , StringType, true), //$20
        StructField("V21SocImage"           , StringType, true), //$21
        StructField("V21SocVideo"           , StringType, true), //$22
        StructField("V21Quotations"         , StringType, true), //$23
        StructField("V21AllNames"           , StringType, true), //$24
        StructField("V21Amounts"            , StringType, true), //$25
        StructField("V21TransInfo"          , StringType, true), //$26
        StructField("V2ExtrasXML"           , StringType, true)  //$27
        ))

	val gkg_file_raw = sqlContext.read
								 .format("com.databricks.spark.csv")
                                 .option("header", "false")             // first line of csv's is not a header
                                 .schema(GkgCoreSchema)
                                 .option("delimiter", "\t")             // tab delimited input
                                 .load(textFileName)

    gkg_file_raw.registerTempTable("gkg_file_raw")

    val gcam_raw = gkg_file_raw.select("GkgRecordID","V21Date","V15Tone","V2GCAM", "V1Locations")
    	gcam_raw.cache()
    	gcam_raw.registerTempTable('gcam_raw')

    // wrap the imported geohash funciton to register as a udf

    def geo_wrap(lat: Double, long: Double, len: Int): String = {
    	var ret = GeoHash.encodeHash(lat, long, len)
    	// geohashes under 12 characters will have accuracy of a couple meters
    	return(ret)
    }

    sqlContext.udf.register("geo_hash", geo_wrap(_:Double, _:Double, _:Int))

    val extract_gcam = sqlContext.sql('''
    	SELECT 
    		GkgRecordID 
        ,   V21Date
        ,   split(V2GCAM, ",")                  AS array        -- split into array for later selection
        ,   explode(split(V1Locations, ";"))    AS locArray     -- I pick up all the locations and later filter.
        ,   regexp_replace(V15Tone, ",.*$", "") AS V15Tone      -- I trunc off the other scores
        FROM gcam_raw
        WHERE
        	length(V2GCAM) > 1
        AND length(V1Locations) > 1  -- only use valid entries
    ''')

    val explode_gcam_df = extract_gcam.explode("array", "gcamRow"){c: Seq[String] => c}

    val gcam_rows = explode_gcam_df.select("GkgRecordID", "V21Date", "V15Tone", "gcamRow"
		, "locArray")
    gcam_rows.registerTempTable("gcam_rows")

    val time_series = sqlContext.sql('''
    	SELECT
    		d.V21Date
    	, 	d.LocCountryCode
    	, 	d.Lat 
    	,	d.Long 	
    	, 	geo_hash(d.Lat, d.Long, 12) 		AS GeoHash
    	, 	'E' 								AS NewsLang
    	, 	regexp_replace(Series, "\\.", "_") 	AS Series       -- replace any periods with underscores to avoid problems
    	, 	COALESCE(sum(d.Value), 0) 			AS SumValue     -- replace nulls with 0
    	, 	COUNT(DISTINCT GkgRecordID) 		AS ArticleCount -- for normalization
    	, 	AVG(V15Tone) 						AS AvgTone
    	FROM
    	(	SELECT
    			GkgRecordID
    		,	V21Date
    		,	ts_array[0] AS Series
    		, 	ts_array[1] AS Value
    		, 	loc_array[0] as LocType
        	, 	loc_array[2] as LocCountryCode
        	, 	loc_array[4] as Lat
        	, 	loc_array[5] as Long
        	, 	V15Tone
        	FROM
        	( 	SELECT
        			GkgRecordID
        		, 	V21Date
        		,	split(gcamRow, ":")  AS ts_array
        		,	split(locArray, "#") AS loc_array
        		,	V15Tone
        		FROM gcam_rows
        		WHERE length(gcamRow) > 1
        	) x 
        	WHERE
        	(loc_array[0] = 3 or loc_array[0] = 4)  -- 3 and 4 are for cities in the US and world cities.
    	) d
    	GROUP BY
    		d.V21Date
    	,	d.LocCountryCode
    	,	d.Lat
    	,	d.Long
    	,	GeoHash
    	,	Series
    	ORDER BY
    		d.V21Date
    	,	GeoHash
    	, 	Series
    	''')
    	time_series.repartition(10)
    		return time_series
}


val TimeSeries = extract_time_series("s3://raw-gkg-gdelt/*")

TimeSeries.write.parquet("s3://gdelt.storage/")