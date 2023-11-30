from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from pyspark.sql.types import *
from pyspark import SparkConf
import time
import threading
import xml.etree.ElementTree as ET
import boto3
from datetime import datetime, timedelta
from abc import ABC, abstractmethod

partition = 64
spark_conf = SparkConf()
spark_conf.set("spark.scheduler.mode", "FAIR")
spark_conf.set("spark.locality.seconds", "1")
spark_conf.set("spark.sql.shuffle.partitions", "{}".format(partition))
spark_conf.set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
spark_conf.set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")

sc = SparkContext(conf=spark_conf)
glueContext = GlueContext(sc)
glueContext._jsc.hadoopConfiguration().set("mapred.output.committer.class",
                                           "org.apache.hadoop.mapred.DirectFileOutputCommitter")
spark = glueContext.spark_session
args = getResolvedOptions(sys.argv,
                          ['JOB_NAME', 'START_DATE', 'FORMAT', 'CHANNELS', 'END_DATE', 'REQUEST_ID', 'REQUEST_TYPE', 'RECEIPT_HANDLE', "QUEUE_URL"])
job = Job(glueContext)
logger = glueContext.get_logger()
job.init(args['JOB_NAME'], args)
request_id = args["REQUEST_ID"]
format_name = args["FORMAT"]
channel = args["CHANNELS"].split(",")
start_date = args["START_DATE"]
end_date = args["END_DATE"]
request_type = args["REQUEST_TYPE"]
reciept_handler = args["RECEIPT_HANDLE"]
queue_url = args["QUEUE_URL"]

year = [start_date.split("-")[0], end_date.split("-")[0]]
month = [start_date.split("-")[1], end_date.split("-")[1]]

sdate = datetime.strptime(start_date, "%Y-%m-%d") - timedelta(days=1)
edate = datetime.strptime(end_date, "%Y-%m-%d") + timedelta(days=1)
date_list = [
    datetime.strftime(sdate + timedelta(days=x), "%Y-%m-%d")
    for x in range((edate - sdate).days)
]

year = list(set([i.split("-")[0] for i in date_list]))
month =  list(set([i.split("-")[1] for i in date_list]))

channel_name = '(' + ",".join(f"'{i}'" for i in channel) + ')'
prev_start = datetime.strftime(datetime.strptime(start_date, "%Y-%m-%d") - timedelta(days=1), "%Y-%m-%d")
after_end = datetime.strftime(datetime.strptime(end_date, "%Y-%m-%d") + timedelta(days=1), "%Y-%m-%d")

# s3_path = "s3a://udp-milestone3-5-np/udp-deltalake-np/ned/"
# s3_path_udp = "s3a://udp-milestone3-5-np/udp-deltalake-np/udp/"

config = [
    {'key': 's3_path', 'value': 's3a://udp-milestone3-5-np/udp-deltalake-np/ned/'},
    {'key': 's3_path_udp','value': 's3a://udp-milestone3-5-np/udp-deltalake-np/udp/'}
] 

class ReaderStrategy(ABC):
    @abstractmethod
    def read_data(self, config):
        pass
    
class WriterStrategy(ABC):
    @abstractmethod
    def write_data(self, config):
        pass
    
class DefaultReaderStrategy(ReaderStrategy):
    def read_data(self, config):
        
        for action in config:
            if action['key'] == 's3_path': 
                s3_path = action['value']
            if action['key'] == 's3_path_udp': 
                s3_path_udp = action['value']
        
        print(s3_path)   
        print(s3_path_udp)
        spark.sparkContext.setLocalProperty("spark.scheduler.pool", "bigger_entity_1")

        spark.read.format("delta").load(f"{s3_path}AiringRelationship/").select(
            "batchId", "gId", "relationshipType", "targetGId", "dnu", "targetGType").filter(
            (col("targetGType").isin(["VideoWorkPresentation", "ProgrammingSource", "Vocabulary", "KeyValueBlob"]))).filter(
            col("dnu") == False
        ).createOrReplaceTempView("AiringRelationship")


        spark.read.format("delta").load(f"{s3_path}Airing/").select(
            col("batchId").alias("batchid"),
            col("gId").alias("gid"),
            "dnu",
            "scheduleDay",
            "startTime",
            "duration",
            "qualifiers",
        ).filter(
            col("scheduleDay").isin(date_list)
        ).createOrReplaceTempView(
            "Airing"
        )


        spark.read.format("delta").load(f"{s3_path}VideoWorkPresentation/").select(
            "gId", "batchId", "dnu", "typeCode", "subTypeCode", "context"
        ).filter(col("dnu") == False).repartition(partition, "gId").cache().createOrReplaceTempView(
            "VideoWorkPresentation"
        )



        spark.read.format("delta").load(
            f"{s3_path}VideoWorkPresentationRelationship/"
        ).select(
            "batchId", "gId", "relationshipType", "targetGId", "dnu", "targetGType"
        ).filter(
            (
                col("targetGType").isin(
                    ["Title", "Description", "Media", "VideoWorkPresentation","XidMapping"]
                )
            )
        ).filter(
            col("dnu") == False
        ).repartition(partition, "gId").createOrReplaceTempView(
            "VideoWorkPresentationRelationship"
        )



        spark.read.format("delta").load(f"{s3_path}Title/").filter(
            col("dnu")  == False).select(
            "batchId", "gId", "dnu", "typeCode", "isMain", "text", "context"
        ).createOrReplaceTempView("Title")



        spark.read.format("delta").load(f"{s3_path}ProgrammingSource/").filter(
            col("dnu")  == False
        ).select(
            "batchId",
            "gId",
            "dnu",
            "typeCode",
            "name",
            "countriesofcoverage",
            "editinglanguages",
            "contentLanguages",
            "timezone"
        ).createOrReplaceTempView(
            "ProgrammingSource"
        )



        spark.read.format("delta").load(f"{s3_path}ProgrammingSourceRelationship/").select(
            col("gId"),
            col("batchId"),
            col("relationshipType"),
            col("targetGId"),
            "dnu",
            col("targetGType"),
        ).filter(
            (
                col("targetGType").isin(["ProgrammingSourceName", "XidMapping"])
                
            )
        ).filter(
            col("dnu")  == False
        ).createOrReplaceTempView(
            "ProgrammingSourceRelationship"
        )



        spark.read.format("delta").load(f"{s3_path}ProgrammingSourceName/").filter(
            col("dnu")  == False
        ).select(
            col("batchId"), col("gId"), "dnu", "text", col("typeCode"), "context"
        ).createOrReplaceTempView(
            "ProgrammingSourceName"
        )



        spark.read.format("delta").load(f"{s3_path}XidMapping/").select(
            "batchId", "gId", "dnu", "idType", "idSource", "xId"
        ).filter(
            col("dnu")  == False
        ).createOrReplaceTempView(
            "XidMapping"
        )



        spark.read.format("delta").load(f"{s3_path}Description/").select(
            "batchId", "gId", "dnu", "typeCode", "subTypeCode", "text", "context"
        ).filter(
            col("subTypeCode") == ""
        ).filter(
            col("dnu")  == False
        ).createOrReplaceTempView(
            "Description"
        )



        spark.read.format("delta").load(f"{s3_path}VideoWorkVersionRelationship/").select(
            "batchId", "gId", "relationshipType", "targetGId", "dnu", "targetGType"
        ).filter(
            (col("targetGType").isin(["VideoWorkPresentation","XidMapping"]))
        ).filter(
            col("dnu")  == False
        ).createOrReplaceTempView(
            "VideoWorkVersionRelationship"
        )



        spark.read.format("delta").load(f"{s3_path}VideoWorkRootRelationship/").filter(
            (
                col("targetGType").isin(
                    ["Keyword", "Media", "PersonCredit", "Title", "VideoWorkVersion","CompanyCredit","Vocabulary","XidMapping","KeyValueBlob"]
                )
            )
            
        ).filter(col("dnu")  == False).select(
            col("batchId"),
            col("gId"),
            col("relationshipType"),
            col("targetGId"),
            "dnu",
            col("targetGType"),
        ).repartition(partition, "gId").createOrReplaceTempView(
            "VideoWorkRootRelationship"
        )



        spark.read.format("delta").load(f"{s3_path}VideoWorkRoot/").filter(
            col("dnu")  == False
        ).select(
            col("batchId"),
            col("gId"),
            "dnu",
            col("typeCode"),
            col("subTypeCode"),
            col("countriesOfOrigin"),
            col("audioLanguages"),
            col("releaseYear"),
            "genre",
            col("originalAirDate"),
        ).repartition(partition, "gId").cache().createOrReplaceTempView(
            "VideoWorkRoot"
        )



        spark.read.format("delta").load(f"{s3_path}VideoWorkVersion/").filter(
            col("dnu")  == False
        ).select(
            col("batchId"),
            col("gId"),
            "dnu",
            col("typeCode"),
            col("subTypeCode"),
            col("versionLabels"),
            col("audioLanguages"),
            "duration",
        ).repartition(partition, "gId").cache().createOrReplaceTempView(
            "VideoWorkVersion"
        )



        spark.read.format("delta").load(f"{s3_path}Keyword/").filter(
            col("dnu")  == False
        ).select("batchId", "gId", "dnu", "category", "text").createOrReplaceTempView(
            "Keyword"
        )


        spark.read.format("delta").load(f"{s3_path_udp}Classification/").filter(
            col('dnu')  == False).createOrReplaceTempView(
            "Classification")


        spark.read.format("delta").load(f"{s3_path_udp}Vocabulary/").select(
            "batchId", "gId", "classificationid", "dnu", "value", "abbrv", "description"
        ).filter(
            col("dnu")  == False
        ).createOrReplaceTempView(
            "Vocabulary"
        )


        voc1 = spark.read.format("delta").load(f"{s3_path}VocabularyRelationship/").select(
            "batchId", "gId", "relationshipType", "targetGId", "dnu", "targetGType"
        ).filter(col("targetGType").isin(["LocalizedText"])).filter(
            col("dnu")  == False
        )

        voc2 = spark.read.format("delta").load(f"{s3_path_udp}VocabularyRelationship/").select(
            "batchId", "gId", "relationshipType", "targetGId", "dnu", "targetGType"
        ).filter(col("targetGType").isin(["Classification"])).filter(
            col("dnu")  == False
        )

        voc1.union(voc2).createOrReplaceTempView(
            "VocabularyRelationship_ALL"
        )


        voc1 = spark.read.format("delta").load(f"{s3_path}VocabularyRelationship/").select(
            "batchId", "gId", "relationshipType", "targetGId", "dnu", "targetGType"
        ).filter(col("targetGType").isin(["LocalizedText"])).filter(
            col("dnu")  == False
        ).createOrReplaceTempView(
            "VocabularyRelationship"
        )


        voc2 = spark.read.format("delta").load(f"{s3_path_udp}VocabularyRelationship/").select(
            "batchId", "gId", "relationshipType", "targetGId", "dnu", "targetGType"
        ).filter(col("targetGType").isin(["Classification"])).filter(
            col("dnu")  == False
        ).createOrReplaceTempView(
            "VocabularyRelationship_udp"
        )


        spark.read.format("delta").load(f"{s3_path}CompanyCredit/").select(
            "batchId", "gId", "dnu", "companyId", "group", "role"
        ).filter(col("dnu")  == False).createOrReplaceTempView("CompanyCredit")



        spark.read.format("delta").load(f"{s3_path}Company/").select(
            "batchId", "gId", "dnu", "name", "groups"
        ).filter(col("dnu")  == False).createOrReplaceTempView("Company")



        spark.read.format("delta").load(f"{s3_path}LocalizedText/").select(
            "batchId", "gId", "dnu", "text","locale"
        ).filter(col("dnu")  == False).cache().createOrReplaceTempView(
            "LocalizedText"
        )



        spark.read.format("delta").load(f"{s3_path}PersonCredit/").select(
            "batchId",
            "gId",
            "dnu",
            "group",
            "role",
            "roleText",
            "character",
            "personId",
            "ord",
            "departed",
        ).filter(col("dnu")  == False).filter(
            col("roletext").isin(
                [
                    "ACTOR",
                    "CONTESTANT",
                    "CREATOR",
                    "DIRECTOR",
                    "EXECUTIVE PRODUCER",
                    "GUEST",
                    "HOST",
                    "JUDGE",
                    "NARRATOR",
                    "PRODUCER",
                    "SELF",
                    "VOICE",
                    "WRITER",
                ])
        ).createOrReplaceTempView(
            "PersonCredit"
        )



        spark.read.format("delta").load(f"{s3_path}PersonCore/").select(
            "batchId",
            "gId",
            "dnu",
            "typeCode",
            "gender",
            "dateOfBirth",
            "placeOfBirth",
            "dateOfDeath",
        ).filter(col("dnu")  == False).createOrReplaceTempView("PersonCore")



        spark.read.format("delta").load(f"{s3_path}PersonName/").select(
            "batchId", "gId", "dnu", "isMain", "typeCode", "context", "fullName"
        ).filter(
            col("dnu")  == False
        ).createOrReplaceTempView(
            "PersonName"
        )

        spark.read.format("delta").load(f"{s3_path}PersonCoreRelationship/").select(
            "batchId", "gId", "relationshipType", "targetGId", "dnu", "targetGType"
        ).filter(col("targetGType").isin(["PersonName","XidMapping"]) 
        ).filter(
            col("dnu")  == False
        ).createOrReplaceTempView(
            "PersonCoreRelationship"
        )

        spark.read.format("delta").load(f"{s3_path}VideoWorkGroupMember/").select(
            "batchId",
            "gId",
            "dnu",
            "groupRootId",
            "memberRootId",
            "membershipType",
            "memberSeq",
            "context",
        ).filter(col("dnu")  == False).createOrReplaceTempView("VideoWorkGroupMember")


        spark.read.format("delta").load(f"{s3_path_udp}Country/").filter(
            col("dnu")  == False
        ).select("batchId", "gId", "dnu", "name", "languageinname","isocode","isocode3").createOrReplaceTempView(
            "Country"
        )


        spark.read.format("delta").load(f"{s3_path_udp}CountryRelationship/").filter(
            col("dnu")  == False
        ).createOrReplaceTempView(
            "CountryRelationship"
        )
        
        spark.read.format("delta").load(f"{s3_path}KeyValueBlob/").filter(
            col("dnu")  == False
        ).createOrReplaceTempView(
            "KeyValueBlob"
        )

        
        spark.sql("""
        SELECT c.name, voc.value, voc.gId
        FROM vocabulary voc
        JOIN VocabularyRelationship_udp voc_rel on voc_rel.gid = voc.gid
        JOIN classification c ON c.gid = voc_rel.targetgid
        WHERE
        voc_rel.targetgtype = 'Classification'
        AND voc_rel.relationshiptype = 'classification'
    """).cache().createOrReplaceTempView("controllvoc_temp")

        spark.sql("""
            SELECT 
                Language, 
                Description, 
                cms_language 
            FROM 
                (
                VALUES 
                    ('af', 'Afrikaans', 'Afrikaans'), 
                    ('ar', 'Arabic', 'Arabic'), 
                    ('ar-Latn', 'Arabic [Rom.]', 'English'), 
                    ('az', 'Azerbaijani', 'Azerbaijani'), 
                    ('bg', 'Bulgarian', 'Bulgarian'), 
                    ('bho', 'Bhojpuri', 'Bhojpuri'), 
                    ('bn', 'Bengali', 'Bengali'), 
                    ('bo', 'Tibetan', 'Tibetan'), 
                    ('bs', 'Bosnian', 'Bosnian'), 
                    ('cmn', 'Mandarin', 'Mandarin'), 
                    ('cs', 'Czech', 'Czech'), 
                    ('da', 'Danish', 'Danish'), 
                    ('de', 'German', 'German'), 
                    ('el', 'Greek', 'Greek'), 
                    ('en', 'English US', 'English'), 
                    ('en-AU', 'English Australia', 'English'), 
                    ('en-GB', 'English GB', 'English'), 
                    ('en-IN', 'English India', 'English'), 
                    ('es', 'Spanish', 'Spanish'), 
                    ('es-ES', 'Spanish [Spain]', 'Spanish'), 
                    ('fa', 'Persian', 'Persian'), 
                    ('fi', 'Finnish', 'Finnish'), 
                    ('fil', 'Filipino', 'Filipino'), 
                    ('fr-CA', 'French [Canada]', 'French'), 
                    ('fr-FR', 'French [France]', 'French'), 
                    ('ga', 'Irish', 'Irish'), 
                    ('gu', 'Gujarati', 'Gujarati'), 
                    ('he', 'Hebrew', 'Hebrew'), 
                    ('hi', 'Hindi', 'Hindi'), 
                    ('hr', 'Croatian', 'Croatian'), 
                    ('hu', 'Hungarian', 'Hungarian'), 
                    ('hy', 'Armenian', 'Armenian'), 
                    ('id', 'Indonesian', 'Indonesian'), 
                    ('inc-Latn', 'Indic [Rom.]', 'English'), 
                    ('it', 'Italian', 'Italian'), 
                    ('ja', 'Japanese', 'Japanese'), 
                    ('ja-Latn', 'Japanese [Hepburn Rom.]', 'English'), 
                    ('kn', 'Kannada', 'Kannada'), 
                    ('ko', 'Korean', 'Korean'), 
                    ('ko-Latn', 'Korean [Rom.]', 'English'), 
                    ('ku', 'Kurdish', 'Kurdish'), 
                    ('lv', 'Latvian', 'Latvian'), 
                    ('mk', 'Macedonian', 'Macedonian'), 
                    ('ml', 'Malayalam', 'Malayalam'), 
                    ('mn', 'Mongolian', 'Mongolian'), 
                    ('mr', 'Marathi', 'Marathi'), 
                    ('ms', 'Malay', 'Malay'), 
                    ('mt', 'Maltese', 'Maltese'), 
                    ('ne', 'Nepali', 'Nepali'), 
                    ('nl', 'Dutch', 'Dutch'), 
                    ('no', 'Norwegian', 'Norwegian'),
                    ('ori', 'Oriya', 'Oriya'),
                    ('pa', 'Panjabi', 'Punjabi'), 
                    ('pl', 'Polish', 'Polish'), 
                    ('ps', 'Pashto', 'Pashto'), 
                    ('pt', 'Portuguese [Portugal]', 'Portuguese'), 
                    ('pt-BR', 'Portuguese [Brazil]', 'Portuguese'), 
                    ('ro', 'Romanian', 'Romanian'), 
                    ('ru', 'Russian', 'Russian'), 
                    ('ru-Latn', 'Russian [Rom.]', 'English'), 
                    ('si', 'Sinhala', 'Sinhala'), 
                    ('sr', 'Serbian', 'Serbian'), 
                    ('sv', 'Swedish', 'Swedish'), 
                    ('sw', 'Swahili', 'Swahili'), 
                    ('ta', 'Tamil', 'Tamil'), 
                    ('te', 'Telugu', 'Telugu'), 
                    ('th', 'Thai', 'Thai'), 
                    ('th-Latn', 'Thai Latin', 'English'), 
                    ('tl', 'Tagalog', 'Tagalog'), 
                    ('tr', 'Turkish', 'Turkish'), 
                    ('uk', 'Ukrainian', 'Ukrainian'), 
                    ('ur', 'Urdu', 'Urdu'), 
                    ('vi', 'Vietnamese', 'Vietnamese'), 
                    ('xh', 'Xhosa', 'Xhosa'), 
                    ('yue', 'Cantonese', 'Cantonese'), 
                    ('zh', 'Chinese', 'Chinese'), 
                    ('zu', 'Zulu', 'Zulu')) 
                    AS lang (Language, Description, cms_language)
        """).cache().createOrReplaceTempView("language_temp")

        spark.sql("""
            SELECT 
                typecode, 
                subtypecode, 
                cmsgenre 
            FROM 
                (
                VALUES 
                    ('SHOW', null, 'TV Show'), 
                    ('SHOW', 'MUSIC VIDEO', 'Music'), 
                    ('MOVIE', null, 'Film'), 
                    ('SPORT', null, 'Sport'), 
                    ('TEAM EVENT', null, 'Sport'), 
                    ('NON-TEAM EVENT', null, 'Sport')
                ) AS genre (typecode, subtypecode, cmsgenre)
        """).cache().createOrReplaceTempView("genre_temp")

        spark.sql("""
            SELECT 
                typecode,
                subtypecode,
                cmsgenre,
                cmssubgenre
            FROM 
                (
                VALUES 
                    ('SHOW', null, 'TV Show','Entertainment'), 
                    ('SHOW', null, 'Children','Entertainment'), 
                    ('SHOW', null, 'News','Special Report'), 
                    ('SHOW', null, 'Documentary','Others'), 
                    ('SHOW', null, 'Business And Finance','Special Report'), 
                    ('SHOW', 'MUSIC VIDEO', 'Music','Music'), 
                    ('MOVIE', 'TV MOVIE', 'Film','Tele Film'), 
                    ('MOVIE', 'SHORT FILM', 'Film','Short Film'), 
                    ('MOVIE', 'FEATURE FILM', 'Film','Entertainment'), 
                    ('SPORT', null, 'Sport','Sport')
                ) AS defaultsubgenre (typecode, subtypecode, cmsgenre, cmssubgenre)
        """).cache().createOrReplaceTempView("defaultsubgenre_temp")

        spark.sql("""
        SELECT 
                typecode, 
                nedgenre, 
                cmsgenre, 
                cmssubgenre,
                cmssubgenrel2 
            FROM 
                (
                VALUES 
                    ('SPORT','3x3 Basketball','Sport','Sport',''),
                    ('SPORT','Acrobatics & Tumbling','Sport','Sport',''),
                    ('MOVIE','Action','Film','Action',''),
                    ('SHOW','Action','TV Show','Action',''),
                    ('SPORT','Action sports','Sport','Extreme Sports',''),
                    ('MOVIE','Adventure','Film','Adventure',''),
                    ('SHOW','Adventure','TV Show','Adventure',''),
                    ('SHOW','Adventure','Documentary','Adventure',''),
                    ('SPORT','Adventure','Sport','Adventure',''),
                    ('SPORT','Aerobics','Sport','Aerobics',''),
                    ('SHOW','Agriculture','TV Show','Agriculture/Rural',''),
                    ('SPORT','Alpine skiing','Sport','Winter Sports',''),
                    ('SHOW','Amateur Wrestling','TV Show','',''),
                    ('SHOW','American Football','TV Show','',''),
                    ('SHOW','American history','Documentary','History',''),
                    ('SHOW','American history','Documentary','History',''),
                    ('SHOW','Animals','Documentary','Factual Entertainment',''),
                    ('SHOW','Anthology','TV Show','',''),
                    ('Sport','Archery','Sport','Archery',''),
                    ('SPORT','Arm wrestling','Sport','Wrestling',''),
                    ('SHOW','Art','Documentary','Art',''),
                    ('MOVIE','Art','Film','Art',''),
                    ('SPORT','Artistic Swimming','Sport','Swimming',''),
                    ('SHOW','Arts/crafts','TV Show','Entertainment',''),
                    ('SHOW','Arts/crafts','Documentary','Factual Entertainment',''),
                    ('SHOW','Arts/crafts','Children','Education',''),
                    ('SHOW','Auction','TV Show','Events/Festivals',''),
                    ('SPORT','Australian rules football','Sport','Sport',''),
                    ('SHOW','Auto','Documentary','Factual Entertainment',''),
                    ('SPORT','Auto racing','Sport','Racing',''),
                    ('SHOW','Aviation','Documentary','Engineering',''),
                    ('SHOW','Awards','TV Show','Award Shows',''),
                    ('SPORT','Badminton','Sport','Badminton',''),
                    ('SHOW','Ballet','Documentary','Culture',''),
                    ('SHOW','Ballet','TV Show','Events/Festivals',''),
                    ('SPORT','Bandy','Sport','Winter Sports',''),
                    ('SPORT','Baseball','Sport','Baseball',''),
                    ('SPORT','Basketball','Sport','Basketball',''),
                    ('SPORT','Beach soccer','Sport','Soccer',''),
                    ('SPORT','Beach volleyball','Sport','Volleyball',''),
                    ('SPORT','Biathlon','Sport','Winter Sports',''),
                    ('SPORT','Bicycle','Sport','Cycling',''),
                    ('SPORT','Bicycle racing','Sport','Cycling',''),
                    ('SPORT','Billiards','Sport','Billiards',''),
                    ('SHOW','Biography','Film','Biography/Biopic',''),
                    ('SHOW','Biography','Documentary','Biography',''),
                    ('SPORT','Blackjack','Sport','Card Games',''),
                    ('SPORT','BMX Racing','Sport','Extreme Sports',''),
                    ('SHOW','Boat','Documentary','Factual Entertainment',''),
                    ('SPORT','Boat racing','Sport','Water Sport',''),
                    ('SPORT','Bobsled','Sport','Winter Sports',''),
                    ('SHOW','Boccia','TV Show','',''),
                    ('SPORT','Bodybuilding','Sport','Sport',''),
                    ('SHOW','Books & Literature','Documentary','Art/Culture',''),
                    ('SHOW','Books & Literature','TV Show','Art/Culture',''),
                    ('SHOW','Bowling','Sport','Bowling',''),
                    ('SHOW','Bowls','Sport','Sport',''),
                    ('SHOW','Boxing','Sport','Boxing',''),
                    ('SHOW','Bull riding','Sport','Extreme Sports',''),
                    ('SHOW','Bullfighting','Sport','Extreme Sports',''),
                    ('SHOW','Bus./financial','Business & Finance','',''),
                    ('SHOW','Cabaret','TV Show','Events/Festivals',''),
                    ('SPORT','Camogie','Sport','Sport',''),
                    ('SPORT','Canoe','Sport','Water Sport',''),
                    ('SPORT','Card games','Sport','Card games',''),
                    ('SPORT','Cheerleading','Sport','Cheerleading',''),
                    ('SPORT','Chess','Sport','Chess',''),
                    ('SPORT','Classic Sport Event','Sport','Sport',''),
                    ('SHOW','Collectibles','Documentary','Factual Entertainment',''),
                    ('MOVIE','Comedy','Film','Comedy',''),
                    ('MOVIE','Comedy','Play','Comedy',''),
                    ('SHOW','Comedy','TV Show','Comedy',''),
                    ('SHOW','Comedy','Children','Comedy',''),
                    ('MOVIE','Comedy drama','Film','Comedy','Drama'),
                    ('MOVIE','Comedy drama','Play','Comedy','Drama'),
                    ('SHOW','Comedy drama','TV Show','Comedy','Drama'),
                    ('SHOW','Comedy drama','Children','Comedy',''),
                    ('SHOW','Community','Documentary','Social',''),
                    ('SHOW','Competition Reality','TV Show','Reality - Talent',''),
                    ('SHOW','Competitive Eating','TV Show','',''),
                    ('SHOW','Computers','Documentary','Social',''),
                    ('SHOW','Computers','TV Show','Education [Adult]',''),
                    ('SHOW','Consumer','TV Show','Consumer',''),
                    ('SHOW','Cooking','TV Show','Cooking',''),
                    ('SPORT','Cricket','Sport','Cricket',''),
                    ('MOVIE','Crime','Film','Crime',''),
                    ('MOVIE','Crime','Play','Crime',''),
                    ('SHOW','Crime','TV Show','Crime',''),
                    ('SHOW','Crime','News','Crime',''),
                    ('MOVIE','Crime drama','Film','Crime','Drama'),
                    ('MOVIE','Crime drama','Play','Crime','Drama'),
                    ('SHOW','Crime drama','TV Show','Crime','Drama'),
                    ('SHOW','Crime drama','News','Crime',''),
                    ('SPORT','Cross-country skiing','Sport','Athletics',''),
                    ('SPORT','Curling','Sport','Winter Sports',''),
                    ('SPORT','Cycling','Sport','Cycling',''),
                    ('SHOW','Music, Dance','Music','Dance',''),
                    ('MOVIE','Dark comedy','Film','Comedy',''),
                    ('SHOW','Dark comedy','TV Show','Comedy',''),
                    ('SHOW','Dark comedy','Children','Comedy',''),
                    ('SPORT','Darts','Sport','Darts',''),
                    ('SHOW','Debate','News','Debate',''),
                    ('SPORT','Diving','Sport','Diving',''),
                    ('MOVIE','Docudrama','Film','',''),
                    ('SHOW','Docudrama','Documentary','Docu Drama',''),
                    ('SPORT','Dog racing','Sport','Racing',''),
                    ('SHOW','Dog show','TV Show','Events/Festivals',''),
                    ('SPORT','Dog sled','Sport','Winter Sports',''),
                    ('SHOW','Dokusoap','Documentary','Factual Entertainment','Docu Drama'),
                    ('SPORT','Drag racing','Sport','Racing',''),
                    ('MOVIE','Drama','Film','Drama',''),
                    ('SHOW','Drama','TV Show','Drama',''),
                    ('SHOW','Drama','Children','Drama',''),
                    ('SPORT','Drift racing','Sport','Extreme Sports',''),
                    ('SHOW','Educational','TV Show','Education [Adult]',''),
                    ('SHOW','Educational','Children','Education',''),
                    ('MOVIE','Entertainment','Film','Entertainment',''),
                    ('MOVIE','Entertainment','Play','Entertainment',''),
                    ('SHOW','Entertainment','TV Show','Entertainment',''),
                    ('SHOW','Entertainment','Children','Entertainment',''),
                    ('SHOW','Environment','TV Show','Environment',''),
                    ('SHOW','Environment','Documentary','Environment',''),
                    ('SPORT','Equestrian','Sport','Equestrian',''),
                    ('MOVIE','Erotic','Film','Erotica',''),
                    ('SHOW','Erotic','TV Show','',''),
                    ('SHOW','eSports','Documentary','Sports',''),
                    ('SHOW','Event','TV Show','',''),
                    ('SHOW','Exercise','Documentary','Health and Wellbeing',''),
                    ('SHOW','Exercise','TV Show','Health and Wellbeing',''),
                    ('MOVIE','Fantasy','Film','Fantasy',''),
                    ('SHOW','Fantasy','TV Show','',''),
                    ('SHOW','Fashion','TV Show','Fashion',''),
                    ('SPORT','Fencing','Sport','Fencing',''),
                    ('SPORT','Field hockey','Sport','Hockey',''),
                    ('SPORT','Figure skating','Sport','Winter Sports',''),
                    ('SHOW','Filmreihe','TV Show','',''),
                    ('SPORT','Fishing','Sport','Fishing',''),
                    ('SPORT','Floorball','Sport','Sport',''),
                    ('SPORT','Footvolley','Sport','Sport',''),
                    ('SPORT','Freestyle skiing','Sport','Winter Sports','Adventure'),
                    ('SHOW','Fundraiser','TV Show','Events/Festivals',''),
                    ('SPORT','Futsal','Sport','Football',''),
                    ('SPORT','Gaelic football','Sport','Football',''),
                    ('SHOW','Game show','TV Show','Game show',''),
                    ('SHOW','Game show','Children','Game show',''),
                    ('SPORT','Gaming','Sport','Virtual Gaming',''),
                    ('SHOW','Go','TV Show','Game show',''),
                    ('SHOW','Goalball','TV Show','',''),
                    ('SPORT','Golf','Sport','Golf',''),
                    ('SPORT','Gymnastics','Sport','Gymnastics',''),
                    ('SPORT','Handball','Sport','Handball',''),
                    ('SPORT','Harness racing','Sport','Sport',''),
                    ('SHOW','Health','TV Show','Health and Wellbeing',''),
                    ('SHOW','Health','Documentary','Health and Wellbeing',''),
                    ('SHOW','Historical drama','Film','History','Drama'),
                    ('SHOW','Historical drama','TV Show','History','Drama'),
                    ('SHOW','Historical drama','Documentary','History','Drama'),
                    ('MOVIE','History','Film','History',''),
                    ('SHOW','History','TV Show','History',''),
                    ('SHOW','History','Documentary','History',''),
                    ('SHOW','Home improvement','Documentary','Factual Entertainment',''),
                    ('MOVIE','Horror','Film','Horror',''),
                    ('SHOW','Horror','TV Show','Horror',''),
                    ('SPORT','Horse','Sport','Equestrian',''),
                    ('SPORT','Horse racing','Sport','Equestrian',''),
                    ('SHOW','House/garden','Documentary','Lifestyle',''),
                    ('SHOW','How-to','Documentary','Factual Entertainment',''),
                    ('SHOW','How-to','TV Show','Education [Adult]',''),
                    ('SHOW','How-to','Children','Education',''),
                    ('SPORT','Hunting','Sport','Adventure',''),
                    ('SPORT','Hurling','Sport','Sport',''),
                    ('SPORT','Hydroplane racing','Sport','Water Sport',''),
                    ('SPORT','Ice Hockey','Sport','Hockey',''),
                    ('SPORT','Indoor soccer','Sport','Sport',''),
                    ('SHOW','Interview','News','Interviews',''),
                    ('SHOW','Interview','Business And Finance','Interviews',''),
                    ('SPORT','Intl basketball','Sport','Basketball',''),
                    ('SPORT','Intl hockey','Sport','Hockey',''),
                    ('SPORT','Intl soccer','Sport','Football',''),
                    ('SPORT','Judo','Sport','Martial Arts',''),
                    ('SPORT','Karate','Sport','Martial Arts',''),
                    ('SPORT','Kayaking','Sport','Water Sport',''),
                    ('SHOW','KOMÃ–DIE','TV Show','Comedy',''),
                    ('MOVIE','KOMÃ–DIE','Film','Comedy',''),
                    ('SPORT','Lacrosse','Sport','Sport',''),
                    ('SHOW','Law','Documentary','Facutal Entertainment',''),
                    ('SHOW','Law','TV Show','Education [Adult]',''),
                    ('SHOW','LGBTQ','TV Show','',''),
                    ('MOVIE','LGBTQ ','Film','',''),
                    ('SPORT','Luge','Sport','Winter Sports',''),
                    ('SPORT','Marathon','Sport','Sport',''),
                    ('MOVIE','Martial arts','Film','',''),
                    ('SPORT','Martial arts','Sport','Martial arts',''),
                    ('SHOW','Medical','Documentary','Health and Wellbeing',''),
                    ('SHOW','Medical','TV Show','Health and Wellbeing',''),
                    ('SHOW','Military','Documentary','Facutal Entertainment','Technology'),
                    ('SPORT','Mixed martial arts','Sport','Martial arts',''),
                    ('SPORT','Modern pentathlon','Sport','Multi-Sport Event',''),
                    ('SPORT','Motorcycle','Sport','Racing',''),
                    ('SPORT','Motorcycle racing','Sport','Racing',''),
                    ('SPORT','Motorsports','Sport','Racing',''),
                    ('SPORT','Mountain biking','Sport','Cycling','Adventure'),
                    ('SPORT','Multi-sport event','Sport','Multi-sport event',''),
                    ('MOVIE','Musical','Film','Musical',''),
                    ('SHOW','Musical','Music','Musical',''),
                    ('MOVIE','Musical comedy','Film','Musical','Comedy'),
                    ('SHOW','Musical comedy','Music','',''),
                    ('MOVIE','Mystery','Film','Mystery',''),
                    ('SHOW','Mystery','TV Show','',''),
                    ('SHOW','Nature','Documentary','Nature',''),
                    ('SPORT','Netball','Sport','Sport',''),
                    ('SHOW','News','News','',''),
                    ('SHOW','Newsmagazine','News','Magazine Programme',''),
                    ('SPORT','Nordic combined','Sport','Winter Sports',''),
                    ('SPORT','Olympics','Sport','Multi-Sport Event',''),
                    ('SHOW','Opera','TV Show','Opera',''),
                    ('SHOW','Outdoors','TV Show','Adventure',''),
                    ('SHOW','Parade','TV Show','Events/Festivals',''),
                    ('SPORT','Paralympics','Sport','Multi-Sport Event',''),
                    ('SHOW','Paranormal','Documentary','Facutal Entertainment',''),
                    ('MOVIE','Paranormal','Film','Horror',''),
                    ('SHOW','Parenting','TV Show','Education [Adult]',''),
                    ('SPORT','Pelota vasca','Sport','Sport',''),
                    ('SHOW','Performing arts','TV Show','Art/Culture','Events/Festivals'),
                    ('SPORT','Pickleball','Sport','',''),
                    ('SPORT','Poker','Sport','Card Games',''),
                    ('SHOW','Political satire','TV Show','Politics','Comedy'),
                    ('SHOW','Political Satire','Documentary','Politics','Comedy'),
                    ('SHOW','Politics','News','Special Report',''),
                    ('SHOW','Politics','Documentary','Politics',''),
                    ('SPORT','Polo','Sport','Polo',''),
                    ('SPORT','Pool','Sport','Pool',''),
                    ('SPORT','Pro wrestling','Sport','Wrestling',''),
                    ('SHOW','Public affairs','TV Show','Special Report',''),
                    ('SHOW','Public affairs','Documentary','Political/Social',''),
                    ('SPORT','Racquet','Sport','Sport',''),
                    ('SHOW','Reality','TV Show','Reality Show',''),
                    ('SHOW','Religious','TV Show','Religious',''),
                    ('SHOW','Religious','Documentary','Religious',''),
                    ('SPORT','Rhythmic Gymnastics','Sport','Gymnastics',''),
                    ('SPORT','Ringuette','Sport','Winter Sports',''),
                    ('SPORT','Road Cycling','Sport','Cycling',''),
                    ('SPORT','Rodeo','Sport','Sport',''),
                    ('SPORT','Roller derby','Sport','Skating',''),
                    ('MOVIE','Romance','Film','Romance',''),
                    ('MOVIE','Romance','Play','Romance',''),
                    ('MOVIE','Romantic comedy','Film','Rom-Com',''),
                    ('SHOW','Romantic comedy','TV Show','',''),
                    ('SPORT','Rowing','Sport','Water Sport',''),
                    ('SPORT','Rugby','Sport','Rugby',''),
                    ('SPORT','Rugby league','Sport','Rugby',''),
                    ('SPORT','Rugby union','Sport','Rugby',''),
                    ('SPORT','Running','Sport','Athletics',''),
                    ('SPORT','Sailing','Sport','Water Sport',''),
                    ('SHOW','Science','TV Show','Science',''),
                    ('SHOW','Science','Documentary','Science',''),
                    ('MOVIE','Science fiction','Film','Sci-Fi',''),
                    ('SHOW','Science fiction','TV Show','Sci-Fi',''),
                    ('SHOW','Self improvement','TV Show','Lifestyle','Health and well-being'),
                    ('SHOW','Self improvement','Documentary','Lifestyle','Health and well-being'),
                    ('SPORT','Sepak takraw','Sport','Sport',''),
                    ('SPORT','Shinty','Sport','Sport',''),
                    ('SPORT','Shooting','Sport','Shooting',''),
                    ('SHOW','Shopping','TV Show','Consumer',''),
                    ('SPORT','Short track speed skating','Sport','Winter Sports',''),
                    ('SHOW','Sitcom','TV Show','Sitcom',''),
                    ('SPORT','Skateboarding','Sport','Adventure',''),
                    ('SPORT','Skating','Sport','Adventure',''),
                    ('SPORT','Skeleton','Sport','Extreme Sports',''),
                    ('SPORT','Ski Jumping','Sport','Winter Sports',''),
                    ('SPORT','Skiing','Sport','Winter Sports',''),
                    ('SPORT','Snooker','Sport','Snooker',''),
                    ('SPORT','Snowboarding','Sport','Winter Sports',''),
                    ('SPORT','Snowmobile','Sport','Winter Sports',''),
                    ('SHOW','Soap','TV Show','Drama',''),
                    ('SHOW','Soccer','Sport','Football',''),
                    ('SHOW','Softball','Sport','Sport',''),
                    ('SHOW','Speed skating','Sport','Winter Sports',''),
                    ('SHOW','Sport Climbing','Sport','Adventure',''),
                    ('SHOW','Sports Related','Sport','Sport',''),
                    ('SHOW','Sports talk','Sport','Talk Show',''),
                    ('SHOW','Squash','Sport','Squash',''),
                    ('SHOW','Standup','TV Show','Comedy',''),
                    ('SPORT','Summer Olympics','Sport','Multi-Sport Event',''),
                    ('SPORT','Summer Paralympics','Sport','Multi-Sport Event',''),
                    ('SPORT','Sumo wrestling','Sport','Wrestling',''),
                    ('SPORT','Surfing','Sport','Surfing',''),
                    ('SPORT','Swimming','Sport','Swimming',''),
                    ('SPORT','Swiss Wrestling','','',''),
                    ('SPORT','Synchronized swimming','Sport','Swimming',''),
                    ('SPORT','Table tennis','Sport','Table tennis',''),
                    ('SPORT','Taekwondo','Sport','Martial Arts',''),
                    ('SHOW','Talk','News','Talk Show',''),
                    ('SHOW','Talk','Business And Finance','Talk Show',''),
                    ('SPORT','Talk','Sport','Talk Show',''),
                    ('SHOW','Technology','TV Show','Technology',''),
                    ('SHOW','Technology','Documentary','Technology',''),
                    ('SHOW','Teleroman','TV Show','Drama',''),
                    ('SPORT','Tennis','Sport','Tennis',''),
                    ('SPORT','Teqball','Sport','Sport',''),
                    ('SHOW','Theater','TV Show','',''),
                    ('MOVIE','Thriller','Film','Thriller',''),
                    ('SHOW','Thriller','TV Show','',''),
                    ('SPORT','Track Cycling','Sport','Cycling',''),
                    ('SPORT','Track/field','Sport','Athletics',''),
                    ('SPORT','Trampoline Gymnastics','Sport','Gymnastics',''),
                    ('SHOW','Travel','TV Show','Travel',''),
                    ('SHOW','Travel','Documentary','Travel',''),
                    ('SPORT','Triathlon','Sport','Triathlon',''),
                    ('SHOW','Variety','TV Show','Variety Show',''),
                    ('SPORT','Volleyball','Sport','Volleyball',''),
                    ('MOVIE','War','Film','War',''),
                    ('SHOW','War','Documentary','War',''),
                    ('SPORT','Water polo','Sport','Water Sport',''),
                    ('SPORT','Water skiing','Sport','Water Sport',''),
                    ('SPORT','Watersports','Sport','Water Sport',''),
                    ('SHOW','Weather','News','Weather',''),
                    ('SPORT','Weightlifting','Sport','Sport',''),
                    ('MOVIE','Western','Film','Classics',''),
                    ('SHOW','Western','TV Show','',''),
                    ('SPORT','Winter Olympics','Sport','Multi-Sport Event','Winter Sports'),
                    ('SPORT','Winter Paralympics','Sport','Multi-Sport Event','Winter Sports'),
                    ('SHOW','World history','Documentary','History',''),
                    ('SPORT','Yacht racing','Sport','Yachting',''),
                    ('SHOW','Animated','Children','Animation',''),
                    ('SHOW','Animated','TV Show','Animation',''),
                    ('MOVIE','Animated','Film','Animation',''),
                    ('SHOW','ANIME','TV Show','Animation',''),
                    ('SPORT','ACROBATICS & TUMBLING','Sport','Sport',''),
                    ('SHOW','ALTERNATIVE','','',''),
                    ('SHOW','ANCIENT HISTORY','Documentary','History',''),
                    ('SHOW','BLUEGRASS','Music','Music',''),
                    ('SHOW','BLUES','Music','Music',''),
                    ('SHOW','CHILDREN-MUSIC','Music','Music',''),
                    ('SHOW','CHILDREN-SPECIAL','Children','Entertainment',''),
                    ('SHOW','CHILDREN-TALK','Children','Entertainment',''),
                    ('SHOW','CLASSICAL','Music','Music',''),
                    ('SHOW','COMPETITIVE EATING','TV Show','Entertainment',''),
                    ('SHOW','CONCERT','Music','CONCERT',''),
                    ('SHOW','COUNTRY','Music','Music',''),
                    ('SHOW','CROSS COUNTRY','Sport','Athletics',''),
                    ('SHOW','DANCE','Music','DANCE',''),
                    ('SHOW','EASY LISTENING','Music','Music',''),
                    ('SHOW','EMO','Music','Music',''),
                    ('MOVIE','FAMILY','Film','FAMILY',''),
                    ('MOVIE','FAMILY','Play','FAMILY',''),
                    ('SHOW','FOLK','Music','Music',''),
                    ('SPORT','FOOTBALL','Sport','FOOTBALL',''),
                    ('SHOW','FRENCH','','',''),
                    ('SHOW','FUNK','Music','Music',''),
                    ('SHOW','GAY/LESBIAN','Documentary','Lifestyle','Culture'),
                    ('SHOW','GAY/LESBIAN','TV Show','Lifestyle','Culture'),
                    ('SHOW','GOSPEL','Music','Music',''),
                    ('SHOW','GOTH','Music','Music',''),
                    ('SHOW','HEAVY METAL','Music','Music',''),
                    ('SHOW','HIP-HOP & RAP','Music','Music',''),
                    ('SPORT','HOCKEY','Sport','HOCKEY',''),
                    ('SHOW','HOLIDAY','TV Show','Events/Festivals',''),
                    ('SHOW','HOLIDAY MUSIC','Music','Music',''),
                    ('SHOW','HOLIDAY MUSIC SPECIAL','Music','Music',''),
                    ('SHOW','HOLIDAY SPECIAL','TV Show','Events/Festivals',''),
                    ('SHOW','HOLIDAY-CHILDREN','Children','Events/Festivals',''),
                    ('SHOW','HOLIDAY-CHILDREN SPECIAL','Children','Events/Festivals',''),
                    ('SHOW','JAZZ','Music','Music',''),
                    ('SHOW','KARAOKE','TV Show','Events/Festivals','Entertainment'),
                    ('SHOW','LATIN','Music','Music',''),
                    ('SHOW','MINISERIES','','',''),
                    ('SHOW','MULTISPORT EVENT','Sport','Multi-Sport Event',''),
                    ('SHOW','MUSIC SPECIAL','Music','Music',''),
                    ('SHOW','MUSIC TALK','Music','Chat/Interactive',''),
                    ('SHOW','PETS','Documentary','Factual Entertainment',''),
                    ('SPORT','PLAYOFF SPORTS','Sport','Sport',''),
                    ('SHOW','POP','Music','POP',''),
                    ('SHOW','R&B','Music','Music',''),
                    ('SHOW','REGGAE','Music','Music',''),
                    ('SHOW','ROCK','Music','Music',''),
                    ('SHOW','SKA','Music','Music',''),
                    ('SHOW','SMOOTH JAZZ','Music','Music',''),
                    ('SHOW','SOAP SPECIAL','TV Show','Drama',''),
                    ('SHOW','SOAP TALK','TV Show','Chat Show',''),
                    ('SHOW','SOFT ROCK','Music','Music',''),
                    ('SHOW','SOUL','Music','Music',''),
                    ('SPORT','SPORTS EVENT','Sport','Sport',''),
                    ('SPORT','SPORTS NON-EVENT','','',''),
                    ('MOVIE','SUSPENSE','Film','Thriller',''),
                    ('SHOW','SWISS WRESTLING','Sport','Wrestling',''),
                    ('SHOW','TECHNO','Music','Music',''),
                    ('SHOW','WORLD','Music','Music',''),
                    ('SPORT','WRESTLING','Sport','WRESTLING','')
            ) as cmsgenre (typecode, nedgenre, cmsgenre, cmssubgenre, cmssubgenrel2)
        """).cache().createOrReplaceTempView("cmsgenre_temp")

        spark.sql("""
                select xm.xid as prgSvcId, ch.gid as channelgId, ch.name as ChannelName, cn.callSign
                , chl.cms_language AS channelLanguage, voc_cl.value as channelLanguageISOCode
                FROM 
                (
                    Select xm.gid, xm.xid
                    FROM controllvoc_temp voc
                    JOIN xidmapping xm ON xm.idtype = voc.gid

                    where voc.name = 'xid type'
                    and voc.value = 'prgSvcId'       
                    and  xm.xid IN """+channel_name+""" 
                ) xm
                JOIN ProgrammingSourceRelationship cr on cr.targetgid = xm.gid
                JOIN ProgrammingSource ch on ch.gid = cr.gid    
                LEFT JOIN controllvoc_temp voc_cl ON voc_cl.gid = element_at(ch.contentLanguages, 1) and voc_cl.name = 'language code'
                LEFT JOIN language_temp chl ON chl.Language = voc_cl.value
                LEFT JOIN LATERAL (SELECT cn.text as callSign
                            FROM ProgrammingSourceRelationship cr_name
                            join ProgrammingSourceName cn on cn.gid  = cr_name.targetgid
                            join controllvoc_temp vt on vt.gid = cn.typecode
                            WHERE cr_name.targetgtype='ProgrammingSourceName' 
                            and cr_name.gid = ch.gid
                            and cr_name.relationshiptype = 'callSign' 
                            and vt.value = 'CallSign'
                            and vt.name = 'programmingSourceNameType'
                ) cn
        """).repartition(partition).createOrReplaceTempView("channel_temp")

        spark.sql("""
                SELECT ar_ch.gid As airingId, ch.prgSvcId as channelId, ch.channelgId, ch.ChannelName, series.groupPresentationgId
                ,  ar_pre.targetgid as presentationgId
                , from_utc_timestamp(a.starttime, 'IST') AS starttime
                , a.duration
                , try_cast(substring(a.duration, instr(a.duration, 'H')+ 1,(instr(a.duration, 'M')- instr(a.duration, 'H'))-1) as int) 
                + (
                    try_cast(substring(a.duration, instr(a.duration, 'T')+ 1, (instr(a.duration, 'H')- instr(a.duration, 'T'))-1) as int) * 60
                    ) As durationinmin
                , IF((array_contains(a.qualifiers, 'REPEAT') OR array_contains(a.qualifiers, 'AUTO REPEAT')),0,1) As IsOriginal
                , IF((array_contains(a.qualifiers, 'GENERIC')), 'P', 'A') As ScheduleType
                , IF(array_contains(a.qualifiers, 'PREMIERE') OR array_contains(a.qualifiers, 'SEASON PREMIERE') OR
            array_contains(a.qualifiers, 'NEW') OR array_contains(a.qualifiers, 'SERIES PREMIERE'), 1, 0) as IsPremier
                , bc.BCCertificate
                , ch.channelLanguage as dubbedLanguage, ch.channelLanguageISOCode as dubbedLanguageISOCode
                , voc_tc.value as typecode, voc_stc.value as subtypecode
                FROM Airing a
                JOIN Airingrelationship ar_ch ON ar_ch.gid = a.gid 
                JOIN channel_temp ch ON ch.channelgId = ar_ch.targetgid
                JOIN Airingrelationship ar_pre ON ar_pre.gid = a.gid
                JOIN VideoWorkPresentation vwp ON vwp.gid = ar_pre.targetgid
                JOIN controllvoc_temp voc_tc ON voc_tc.gid = vwp.typecode and voc_tc.name = 'program type'
                JOIN controllvoc_temp voc_stc ON voc_stc.gid = vwp.subtypecode and voc_stc.name = 'program subtype'
                LEFT JOIN LATERAL(
                    SELECT /*+ REPARTITION(64) */  rtseries.targetgid AS groupPresentationgId 
                    FROM VideoWorkPresentationRelationship rtseries 
                    WHERE rtseries.gid = ar_pre.targetgid
                    AND rtseries.targetgtype = 'VideoWorkPresentation' 
                    AND rtseries.relationshiptype = 'series'
                ) series

                LEFT JOIN LATERAL (
                    SELECT voc_bc.value as BCCertificate
                    FROM Airingrelationship ar_bc 
                    JOIN Vocabulary voc_bc ON voc_bc.gid = ar_bc.targetgid
                    WHERE ar_bc.gid = a.gid
                    AND ar_bc.targetgtype = 'Vocabulary' 
                    AND ar_bc.relationshiptype = 'airingRating'    
                ) bc

                WHERE ar_ch.targetgtype = 'ProgrammingSource' AND ar_ch.relationshiptype = 'programmingSource' 
                AND ar_pre.targetgtype = 'VideoWorkPresentation' AND ar_pre.relationshiptype = 'videoWorkPresentation'
        """).cache().createOrReplaceTempView("airing_temp")

        spark.sql("""
            SELECT up.presentationgId, tt.tmsId 
                FROM ( 
                Select st.groupPresentationgId As presentationgId 
                From airing_temp st 
                UNION 
                Select at.presentationgId As presentationgId 
                From airing_temp at
                ) up 
                join lateral (
                SELECT tmsid.xid AS TMSID 
                FROM VideoWorkPresentationRelationship rtmsid 
                JOIN XidMapping tmsid ON tmsid.gid = rtmsid.targetgid
                --JOIN controllvoc_temp voc_tsm ON voc_tsm.gid = tmsid.idtype and voc_tsm.name = 'xid type'
                WHERE rtmsid.gid = up.presentationgId 
                    AND rtmsid.targetgtype = 'XidMapping' 
                    AND rtmsid.relationshiptype = 'tmsId' 
                    --AND voc_tsm.value = 'tmsId'
                ) tt
        """).createOrReplaceTempView("tmsid_temp")

        spark.sql("""
        SELECT vwr.gid AS rootgId, vwv.gid AS versiongId, vwp.gid AS presentationgId, 
                voc_tc.value as typeCode, voc_stc.value as subtypecode, pl.cms_language AS programLanguage, voc_cl.value as programLanguageISOCode,
                COALESCE(year(try_cast(vwr.releaseyear as date)),try_cast(vwr.releaseyear as bigint)) As ProductionYear, 
                vwr.genre, vwp.context AS tms_language, vwr.countriesOfOrigin, tt.tmsId

                FROM tmsid_temp tt 
                JOIN VideoWorkPresentation vwp ON vwp.gid = tt.presentationgId
                JOIN VideoWorkVersionRelationship vwv_rel ON vwv_rel.targetgid = vwp.gid
                JOIN VideoWorkVersion vwv ON vwv_rel.gid = vwv.gid
                JOIN VideoWorkRootRelationship vwr_rel ON vwv.gid = vwr_rel.targetgid
                JOIN VideoWorkRoot vwr ON vwr_rel.gid = vwr.gid
                JOIN controllvoc_temp voc_tc ON voc_tc.gid = vwp.typecode and voc_tc.name = 'program type'
                JOIN controllvoc_temp voc_stc ON voc_stc.gid = vwp.subtypecode and voc_stc.name = 'program subtype'
                LEFT JOIN controllvoc_temp voc_cl ON voc_cl.gid = element_at(vwr.audiolanguages, 1) and voc_cl.name = 'language code'
                LEFT JOIN language_temp pl ON pl.Language = voc_cl.value

                WHERE vwv_rel.targetgtype = 'VideoWorkPresentation' 
                AND vwv_rel.relationshiptype = 'videoWorkPresentation'
                AND vwr_rel.targetgtype = 'VideoWorkVersion' 
                AND vwr_rel.relationshiptype = 'videoWorkVersion'
        """).cache().createOrReplaceTempView("videoworkabstraction_temp")

        spark.sql("""
        SELECT rootgId as rootgId, typecode, subtypecode, cmsgenre, subgenre, ARRAY_DISTINCT(targetAudience) as targetAudience,
                CASE gen.cmsgenre 
                    WHEN 'TV Show' THEN 
                        CASE WHEN array_contains(gen.targetAudience, 'Children') THEN 'Children'
                            WHEN array_contains(gen.subgenre, 'News') THEN 'News' 
                            WHEN array_contains(gen.subgenre, 'Music') THEN 'Music' 
                            WHEN array_contains(gen.subgenre, 'Documentary') THEN 'Documentary' 
                            WHEN array_contains(gen.subgenre, 'BUS./FINANCIAL') THEN 'Business And Finance'
                        ELSE gen.cmsgenre END
                ELSE gen.cmsgenre END AS m2ecategoryname
                FROM (
                        select vwr.rootgId, typecode, subtypecode, genre, cmsgenre       
                        , TRANSFORM(ARRAY_SORT(ARRAY_AGG(CASE WHEN voc.name In ('genre')  THEN struct(vwr.index, l.text) END))
                                , sorted_struct -> sorted_struct.text) as subgenre
                        , TRANSFORM(ARRAY_SORT(ARRAY_AGG(CASE WHEN voc.name In ('target audience')  THEN struct(vwr.index, l.text) END))
                                , sorted_struct -> sorted_struct.text) as targetAudience
                        from (
                            SELECT DISTINCT vwr.rootgId, vwr.genre, vwr.typecode, vwr.subtypecode, gn.cmsgenre
                            , posexplode_outer(vwr.genre) AS (index, genereId)
                            FROM videoworkabstraction_temp vwr
                            JOIN genre_temp gn ON UPPER(vwr.typecode) = UPPER(gn.typecode) 
                                            AND UPPER(vwr.subtypecode) = UPPER(COALESCE(gn.subtypecode, vwr.subtypecode)) 
                            WHERE lower(vwr.typecode) IN ('movie', 'show', 'sport')
                        ) vwr
                        JOIN VideoWorkRootRelationship vwrgenre ON vwrgenre.gid = vwr.rootgId       
                        JOIN controllvoc_temp voc ON voc.gid = vwrgenre.targetgid 
                                            AND 1 = (
                                                    case when voc.name In ('genre') AND voc.gid = vwr.genereId  then 1
                                                        when voc.name In ('target audience') then 1
                                                    end
                                                    ) 
                        JOIN VocabularyRelationship r ON r.gid = voc.gid                                         
                        JOIN LocalizedText l ON r.targetgid = l.gid
                        JOIN controllvoc_temp voc_locl ON voc_locl.gid = l.locale and voc_locl.name = 'language code' and voc_locl.value = 'en'
                        Where vwrgenre.targetgtype in ('Vocabulary') 
                        AND vwrgenre.relationshiptype In ('genre', 'targetAudience')    
                        AND r.targetgtype in ('LocalizedText') 
                        AND r.relationshiptype In ('translation') 
                        AND l.text not in ('Special') 
                        AND l.text is not null 

                        group by vwr.rootgId, genre, typecode, subtypecode, cmsgenre
                ) gen
        """).createOrReplaceTempView("genre_mapping_temp")

        spark.sql("""
            SELECT fn_gen.rootgId, fn_gen.typecode, fn_gen.subtypecode, fn_gen.cmsgenre, fn_gen.m2ecategoryname
                , TRANSFORM(ARRAY_SORT(ARRAY_AGG(struct(fn_gen.index, fn_gen.cmssubgenre))), sorted_struct -> sorted_struct.cmssubgenre) as cmssubgenre
                FROM (
                    SELECT gen.rootgId, gen.typecode, gen.subtypecode, gen.cmsgenre, gen.m2ecategoryname, gen.index
                    , concat_ws(', ', ct.cmssubgenre, NULLIF(cmssubgenrel2,'')) as cmssubgenre
                    FROM (
                        SELECT gen_map.rootgId, gen_map.typecode, gen_map.subtypecode, gen_map.cmsgenre, gen_map.m2ecategoryname, gen_map.subgenre
                        , posexplode_outer(gen_map.subgenre) AS (index, genereId) 
                        FROM genre_mapping_temp gen_map

                    ) gen
                    JOIN cmsgenre_temp ct on lower(gen.m2ecategoryname) = lower(ct.cmsgenre) and lower(gen.genereId) = lower(ct.nedgenre)
                    WHERE ct.cmssubgenre !=''        
                ) fn_gen 
                GROUP BY fn_gen.rootgId, fn_gen.typecode, fn_gen.subtypecode, fn_gen.cmsgenre, fn_gen.m2ecategoryname
        """).createOrReplaceTempView("cmsgenre_mapping_temp")

        spark.sql("""
            SELECT rt.gid as presentationgId, 
                max_by(t.text, IF(UPPER(voc_title.value) = 'MAIN', 1, 0)) AS Title,
                max_by(t.context, IF(UPPER(voc_title.value) = 'MAIN', 1, 0)) AS titleLang
                FROM Title t              
                JOIN controllvoc_temp voc_title ON voc_title.gid = t.typecode 
                JOIN VideoWorkPresentationRelationship rt ON rt.targetgid = t.gid  
                WHERE voc_title.name = 'title type'
                --AND upper(voc_title.value) in ('MAIN','AKA')
                AND rt.targetgtype = 'Title' 
                AND rt.relationshiptype In ('childTitle', 'parentTitle') 
                GROUP BY rt.gid
        """).createOrReplaceTempView("title_temp")

        spark.sql("""
            SELECT presentationgId,desclang,
                COALESCE(plot_STB_desc, generic_STB_desc) as STB_desc,
                COALESCE(plot_Short_desc, generic_Short_desc) as Short_desc,
                prog_Source_desc, episode_Source_desc,
                Descriptors
            FROM (
                SELECT 
                presentationgId, desclang,
                CASE WHEN char_length(Description['Generic Plot 250']) > 200 THEN Description['Generic 100'] ELSE Description['Generic Plot 250'] END as generic_STB_desc,
                CASE WHEN char_length(Description['Generic']) > 90 THEN Description['Generic 60'] ELSE Description['Generic'] END as generic_Short_desc,
                CASE WHEN char_length(Description['Plot 250']) > 200 THEN Description['Plot 100'] ELSE Description['Plot 250'] END as plot_STB_desc,
                CASE WHEN char_length(Description['Plot 100']) > 90 THEN Description['Plot 60'] ELSE Description['Plot 100'] END as plot_Short_desc,
                COALESCE(Description['Provider Generic 250'], Description['Provider Generic 100']) as prog_Source_desc,
                COALESCE(Description['Provider Plot 500'], COALESCE(Description['Provider Plot 100'], COALESCE(Description['Provider Plot 60'], Description['Provider Plot 40']))) as episode_Source_desc,
                Description['Source Adaptation'] as Descriptors
                FROM 
                (
                SELECT 
                    ddrw.presentationgId, ddrw.desclang, map_from_arrays(collect_list(ddrw.typecode), collect_list(Description)) As Description 
                FROM 
                    (
                    SELECT rd.gid as presentationgId, d.context as desclang, voc_desc.value as typecode, FIRST_VALUE(d.text) AS Description 
                    FROM Description d
                    JOIN controllvoc_temp voc_desc ON voc_desc.gid = d.typecode
                    JOIN VideoWorkPresentationRelationship rd  ON rd.targetgid = d.gid
        
                    WHERE voc_desc.name = 'description type' 
                        AND voc_desc.value in ('Generic Plot 250','Generic','Generic 60','Plot 250','Plot 100','Plot 60',
                            'Provider Generic 250','Provider Generic 100','Provider Plot 500','Provider Plot 100','Provider Plot 60','Provider Plot 40',
                            'Source Adaptation')
                        AND rd.targetgtype = 'Description' 
                        AND rd.relationshiptype In ('description') 
                    GROUP BY rd.gid, d.context, voc_desc.value 
                    ) ddrw 
                    GROUP BY ddrw.presentationgId, ddrw.desclang
                )
            )
        """).createOrReplaceTempView("desc_temp")

        spark.sql("""
            SELECT vwp.presentationgId
                    , vwp_otv.metadatalangcode
                    , vwp_otv.gid as regionalpresentationgId
                    , vwp_otv.context
                    , vwp_otv.tms_language

                    FROM videoworkabstraction_temp vwp

                    JOIN VideoWorkVersionRelationship vwv_rel_ov ON vwv_rel_ov.gid  =  vwp.versiongId

                    JOIN (
                        SELECT vwp_otv.gid, vwp_otv.context, voc_locl.value as metadatalangcode, explode(vwp_otv.context) AS (tms_language)
                        FROM videoworkpresentation vwp_otv             
                        JOIN controllvoc_temp voc_locl ON cardinality(filter(vwp_otv.context, x -> split_part(x, '_', 3) = voc_locl.gid)) > 0
                        WHERE voc_locl.name = 'language code'
                        AND lower(voc_locl.value) IN ('hi','ta','te','kn','bn','ml')
                        
                    ) vwp_otv ON vwp_otv.gid = vwv_rel_ov.targetgid
                    
                    WHERE vwv_rel_ov.targetgtype ='VideoWorkPresentation'
                    AND vwv_rel_ov.relationshiptype = 'videoWorkPresentation'
        """).createOrReplaceTempView("regionalpresentationgId_temp")

        spark.sql("""
        SELECT regionalvwp.presentationgId, regionalvwp.metadatalangcode, reg_ti.Title as programName
                    , reg_desc.STB_desc as STBDescription, reg_desc.Short_desc as shortDescription

                    FROM regionalpresentationgId_temp regionalvwp
                    --JOIN controllvoc_temp voc_locl ON concat('CTX_L_', voc_locl.gid) = regionalvwp.tms_language and voc_locl.name = 'language code'

                    LEFT JOIN title_temp reg_ti ON reg_ti.presentationgId  = regionalvwp.regionalpresentationgId 

                    LEFT JOIN desc_temp reg_desc ON reg_desc.presentationgId  = regionalvwp.regionalpresentationgId 

                    WHERE cardinality(array_intersect(regionalvwp.context, array_intersect(reg_ti.titleLang, COALESCE(reg_desc.desclang, reg_ti.titleLang)))) > 0
                    --AND lower(voc_locl.value) IN ('hi','ta','te','kn','bn','ml')
        """).createOrReplaceTempView("regionaldetails_temp")

class DefaultWriterStrategy(WriterStrategy):
    def write_data(self, config):

        s3_client = boto3.client('s3')
        s3_response = s3_client.get_object(
            Bucket='output-bucket-cms-ned-poc',
            Key='sql/{}.sql'.format(format_name)
        )

        data = spark.sql(s3_response.get('Body').read().decode().format(channel_name, prev_start, after_end))
        print(data)
        data = data.withColumn(
            "date_seq", explode(
                sequence(
                    to_date("starttime"), to_date("endtime"), expr('interval 1 DAY')
                )
            )
        )

        data = data.withColumn(
            "date_to_use",
            when(to_date(col("starttime")).eqNullSafe(col("date_seq")), col("starttime"))
            .otherwise(col("date_seq")))

        data = data.filter(
            (col("date_to_use") >= (lit(start_date).cast(DateType()))) & (
                    col("date_to_use") < (lit(end_date).cast(DateType())))
        )

        data = data.withColumn(
            "starttime", when(to_date(col("starttime")).eqNullSafe(col("date_seq")),
                            col("starttime"))
            .otherwise(
                col("date_seq")
            )).withColumn(
            "endtime", when(to_date(col("endtime")).eqNullSafe(
                col("date_seq")), col("endtime"))
            .otherwise(col("date_seq") + 1)
            )

        data = data.filter(col('starttime') != col('endtime'))
        data = data.withColumn('schedulefromdate', to_date(col("starttime"))).withColumn(
            'scheduletodate', to_date(col("endtime")))
            
        data.coalesce(1).write.mode('overwrite').parquet("s3a://output-bucket-cms-ned-poc/{}/{}/".format(request_type, request_id))
        s3_client.put_object(
            Bucket='output-bucket-cms-ned-poc',
            Key='{}/{}/_SUCCESS'.format(request_type, request_id)
        )
        
class FileWriterContext :
    def __init__(self, config, reader, writer):
        self.reader = reader
        self.writer = writer
        self.config = config

    def process(self):      
        self.reader.read_data(config)
        self.writer.write_data(config)
  

if  format_name == 'APISnapshot':
    reader = DefaultReaderStrategy()
    writer = DefaultWriterStrategy() # this would be replaced by new concrete class

else : 
    reader = DefaultReaderStrategy()
    writer = DefaultWriterStrategy()

file_writer = FileWriterContext(config, reader, writer)
file_writer.process()
job.commit()
spark.stop()
