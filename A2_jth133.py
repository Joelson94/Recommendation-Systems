#DATA420-19S1 Assignment 1
#Import the required classes

from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from pyspark.sql import *
from pyspark.sql import functions as F
import pandas as pd

spark = SparkSession.builder.getOrCreate()
#Update the default configurations
conf = spark.sparkContext._conf.setAll([('spark.executor.memory', '4g'), ('spark.app.name', 'Spark Updated Conf'), ('spark.executor.cores', '2'), ('spark.executor.instances', '4'), ('spark.driver.memory','4g')])
sc = SparkContext.getOrCreate()

N = int(conf.get("spark.executor.instances"))
M = int(conf.get("spark.executor.cores"))
partitions = 4 * N * M
#Data Processing Q1

analysis_summary = spark.read.load('hdfs:///data/msd/main/summary/analysis.csv.gz', 
                      format='com.databricks.spark.csv', 
                      header='true', 
                      inferSchema='true').repartition(partitions)
analysis_summary.count()
#1000000
metadata_summary = spark.read.load('hdfs:///data/msd/main/summary/metadata.csv.gz', 
                      format='com.databricks.spark.csv', 
                      header='true', 
                      inferSchema='true').repartition(partitions)
metadata_summary.count()
#1000000
#Data Processing Q2 (a)

mismatches_schema = StructType([
    StructField('song_id', StringType(),True),
    StructField('song_artist', StringType(),True),
    StructField('song_title', StringType(),True),
    StructField('track_id', StringType(),True),
    StructField('track_artist', StringType(),True),
    StructField('track_title', StringType(),True),
])

with open("/scratch-network/courses/2019/DATA420-19S1/data/msd/tasteprofile/mismatches/sid_matches_manually_accepted.txt","r") as f:
     lines = f.readlines()
     sid_matches_manually_accepted = []
     for line in lines:
         if line.startswith("< ERROR: "):
             a = line[10:28]
             b = line[29:47]
             c,d = line[49:-1].split("  !=  ")
             e,f = c.split("  -  ")
             g,h = d.split("  -  ")
             sid_matches_manually_accepted.append((a,e,f,b,g,h))


matches_manually_accepted = spark.createDataFrame(sc.parallelize(sid_matches_manually_accepted,8),schema=mismatches_schema)
matches_manually_accepted.cache()
matches_manually_accepted.show(10,50)
# 488
with open("/scratch-network/courses/2019/DATA420-19S1/data/msd/tasteprofile/mismatches/sid_mismatches.txt","r") as f:
	lines = f.readlines()
	sid_mismatches = []
	for line in lines:
		if line.startswith("ERROR: "):
			a = line[8:26]
			b = line[27:45]
			c,d = line[47:-1].split("  !=  ")
			e,f = c.split("  -  ")
			g,h = d.split("  -  ")
			sid_mismatches.append((a,e,f,b,g,h))

mismatches = spark.createDataFrame(sc.parallelize(sid_mismatches,64),schema=mismatches_schema)
mismatches.cache()
mismatches.show(10,50)
# 19094

triplets_schema = StructType([
	StructField("user_id", StringType(), True),
	StructField("song_id", StringType(), True),
	StructField("plays", StringType(), True)
])

triplets = (
	spark.read.format("csv")
	.option("header","false")
	.option("delimiter","\t")
	.option("codec","gzip")
	.schema(triplets_schema)
	.load("hdfs:///data/msd/tasteprofile/triplets.tsv")
	.cache()
)
triplets.show(10,50)
# 48373586
# change ids from strings to integers
userid_change = triplets.select('user_id').distinct().select('user_id', F.monotonically_increasing_id().alias('new_user_id'))
songid_change = triplets.select('song_id').distinct().select('song_id', F.monotonically_increasing_id().alias('new_song_id'))
# join dataframes
triplets_int_ids = triplets.join(userid_change, 'user_id').join(songid_change, 'song_id')

mismatches_not_accepted = mismatches.join(matches_manually_accepted, on="song_id", how="left_anti")
triplets_not_mismatched = triplets.join(mismatches_not_accepted, on="song_id", how="left_anti")
triplets = triplets_not_mismatched
triplets.cache()
# Q2 b)
audio_attribute_type_mapping = {
	"NUMERIC": DoubleType(),
	"real": DoubleType(),
	"string": StringType(),
	"STRING": StringType()
}

audio_dataset_names = [
"msd-jmir-area-of-moments-all-v1.0",
"msd-jmir-lpc-all-v1.0",
"msd-jmir-methods-of-moments-all-v1.0",
"msd-jmir-mfcc-all-v1.0",
"msd-jmir-spectral-all-all-v1.0",
"msd-jmir-spectral-derivatives-all-all-v1.0",
"msd-marsyas-timbral-v1.0",
"msd-mvd-v1.0",
"msd-rh-v1.0",
"msd-rp-v1.0",
"msd-ssd-v1.0",
"msd-trh-v1.0",
"msd-tssd-v1.0"
]

audio_dataset_schemas = {}
audio_features = {}
for audio_dataset_name in audio_dataset_names:
	print(audio_dataset_name)

	audio_dataset_path = f"/scratch-network/courses/2019/DATA420-19S1/data/msd/audio/attributes/{audio_dataset_name}.attributes.csv"
	with open(audio_dataset_path, "r") as f:
		rows = [line.strip().split(",") for line in f.readlines()]

	audio_dataset_schemas[audio_dataset_name] = StructType([
		StructField(row[0], audio_attribute_type_mapping[row[1]], True) for row in rows
		])
	audio_features[audio_dataset_name] = (
		spark.read.format("csv")
		.option("header","false")
		.option("codec","gzip")
		.schema(audio_dataset_schemas[audio_dataset_name])
		.load(f"hdfs:///data/msd/audio/features/{audio_dataset_name}.csv/*")
		.repartition(partitions)
		.withColumnRenamed('MSD_TRACKID', 'track_id')
		)
	print(audio_features[audio_dataset_name].count()) 

#Audio Similarity 

#Q1 a)
audio_features["msd-jmir-methods-of-moments-all-v1.0"].describe().show()
+-------+----------------------------------------------+----------------------------------------------+----------------------------------------------+----------------------------------------------+----------------------------------------------+-----------------------------------+-----------------------------------+-----------------------------------+-----------------------------------+-----------------------------------+------------------+
|summary|Method_of_Moments_Overall_Standard_Deviation_1|Method_of_Moments_Overall_Standard_Deviation_2|Method_of_Moments_Overall_Standard_Deviation_3|Method_of_Moments_Overall_Standard_Deviation_4|Method_of_Moments_Overall_Standard_Deviation_5|Method_of_Moments_Overall_Average_1|Method_of_Moments_Overall_Average_2|Method_of_Moments_Overall_Average_3|Method_of_Moments_Overall_Average_4|Method_of_Moments_Overall_Average_5|          track_id|
+-------+----------------------------------------------+----------------------------------------------+----------------------------------------------+----------------------------------------------+----------------------------------------------+-----------------------------------+-----------------------------------+-----------------------------------+-----------------------------------+-----------------------------------+------------------+
|  count|                                        994623|                                        994623|                                        994623|                                        994623|                                        994623|                             994623|                             994623|                             994623|                             994623|                             994623|            994623|
|   mean|                            0.1549817600174637|                            10.384550576952277|                              526.813972439809|                             35071.97543290272|                             5297870.369577217|                0.35084444325313247|                 27.463867987840658|                 1495.8091812075527|                 143165.46163257834|                2.396783048473542E7|              null|
| stddev|                            0.0664621308614302|                            3.8680013938746787|                             180.4377549977523|                            12806.816272955564|                            2089356.4364558011|                0.18557956834383826|                  8.352648595163753|                  505.8937639190226|                  50494.27617103219|                  9307340.299219687|              null|
|    min|                                           0.0|                                           0.0|                                           0.0|                                           0.0|                                           0.0|                                0.0|                                0.0|                                0.0|                          -146300.0|                                0.0|TRAAAAK128F9318786|
|    max|                                         0.959|                                         55.42|                                        2919.0|                                      407100.0|                                       4.657E7|                              2.647|                              117.0|                             5834.0|                           452500.0|                            9.477E7|TRZZZZO128F428E2D4|
+-------+----------------------------------------------+----------------------------------------------+----------------------------------------------+----------------------------------------------+----------------------------------------------+-----------------------------------+-----------------------------------+-----------------------------------+-----------------------------------+-----------------------------------+------------------+

#Correlation between features
from pyspark.mllib.stat import Statistics
df = audio_features["msd-jmir-methods-of-moments-all-v1.0"]
df = df.drop("track_id")
col_names = df.columns
features = df.rdd.map(lambda row: row[0:])
corr_mat=Statistics.corr(features, method="pearson")
corr_df = pd.DataFrame(corr_mat)
corr_df.index, corr_df.columns = col_names, col_names
print(corr_df.to_string())


                                                Method_of_Moments_Overall_Standard_Deviation_1  Method_of_Moments_Overall_Standard_Deviation_2  Method_of_Moments_Overall_Standard_Deviation_3  Method_of_Moments_Overall_Standard_Deviation_4  Method_of_Moments_Overall_Standard_Deviation_5  Method_of_Moments_Overall_Average_1  Method_of_Moments_Overall_Average_2  Method_of_Moments_Overall_Average_3  Method_of_Moments_Overall_Average_4  Method_of_Moments_Overall_Average_5
Method_of_Moments_Overall_Standard_Deviation_1                                        1.000000                                        0.426280                                        0.296306                                        0.061039                                       -0.055336                             0.754208                             0.497929                             0.447565                             0.167466                             0.100407
Method_of_Moments_Overall_Standard_Deviation_2                                        0.426280                                        1.000000                                        0.857549                                        0.609521                                        0.433797                             0.025228                             0.406923                             0.396354                             0.015607                            -0.040902
Method_of_Moments_Overall_Standard_Deviation_3                                        0.296306                                        0.857549                                        1.000000                                        0.803010                                        0.682909                            -0.082415                             0.125910                             0.184962                            -0.088174                            -0.135056
Method_of_Moments_Overall_Standard_Deviation_4                                        0.061039                                        0.609521                                        0.803010                                        1.000000                                        0.942244                            -0.327691                            -0.223220                            -0.158231                            -0.245034                            -0.220873
Method_of_Moments_Overall_Standard_Deviation_5                                       -0.055336                                        0.433797                                        0.682909                                        0.942244                                        1.000000                            -0.392551                            -0.355019                            -0.285966                            -0.260198                            -0.211813
Method_of_Moments_Overall_Average_1                                                   0.754208                                        0.025228                                       -0.082415                                       -0.327691                                       -0.392551                             1.000000                             0.549015                             0.518503                             0.347112                             0.278513
Method_of_Moments_Overall_Average_2                                                   0.497929                                        0.406923                                        0.125910                                       -0.223220                                       -0.355019                             0.549015                             1.000000                             0.903367                             0.516499                             0.422549
Method_of_Moments_Overall_Average_3                                                   0.447565                                        0.396354                                        0.184962                                       -0.158231                                       -0.285966                             0.518503                             0.903367                             1.000000                             0.772807                             0.685645
Method_of_Moments_Overall_Average_4                                                   0.167466                                        0.015607                                       -0.088174                                       -0.245034                                       -0.260198                             0.347112                             0.516499                             0.772807                             1.000000                             0.984867
Method_of_Moments_Overall_Average_5                                                   0.100407                                       -0.040902                                       -0.135056                                       -0.220873                                       -0.211813                             0.278513                             0.422549                             0.685645                             0.984867                             1.000000


#Q1 b)
genre_assignment_schema = StructType([
	StructField("track_id", StringType(), True),
	StructField("genre", StringType(), True)
])

genre_assignment = (
	spark.read.format("csv")
	.option("header","false")
	.option("delimiter","\t")
	.option("codec","gzip")
	.schema(genre_assignment_schema)
	.load("hdfs:///data/msd/genre/msd-MAGD-genreAssignment.tsv")
	.cache()
)
genre_assignment.count()
# 422714
matched_genre = genre_assignment.join(mismatches_not_accepted, on="track_id", how="left_anti")


genre_grouped = (
    matched_genre
    .select(["genre","track_id"])
    .groupby("genre")
    .agg({'track_id': 'count'})
    .orderBy('count(track_id)', ascending=False)
    .withColumnRenamed('count(track_id)', 'count')
    )
genre = genre_grouped.select("genre").collect()
count = genre_grouped.select("count").collect()
genre = [i[0] for i in genre]
count = [i[0] for i in count]
# Outputs
output_path = os.path.expanduser("~/plots")  # M:/plots on windows
if not os.path.exists(output_path):
  os.makedirs(output_path)
# Figure
import numpy as np
import matplotlib.pyplot as plt
f, a = plt.subplots(dpi=300, figsize=(10, 5))
index = np.arange(len(genre))
a.bar(index, count)
plt.xlabel('Genre', fontsize=5)
plt.ylabel('No of Tracks', fontsize=5)
plt.xticks(index, genre, fontsize=5, rotation=30)
plt.title('Distribution of each Music genre')
# Save
plt.tight_layout()  # reduce whitespace
f.savefig(os.path.join(output_path, f"genre_count.png"), bbox_inches="tight")  # save as png and view in windows
plt.close(f)


audio_features["msd-jmir-methods-of-moments-all-v1.0"] = audio_features["msd-jmir-methods-of-moments-all-v1.0"].withColumn('track_id', F.regexp_replace('track_id',"'",''))

genre_audio = matched_genre.join(audio_features["msd-jmir-methods-of-moments-all-v1.0"],
	on= "track_id",how = "inner").drop(audio_features["msd-jmir-methods-of-moments-all-v1.0"].track_id)
#Q2 b)
electronic_genre = genre_audio.withColumn("genre",
                           F.when(
                               (F.col('genre') == "Electronic"),
                               "Electronic"
                           ).otherwise("Other")
                           )

numeric_features = [t[0] for t in genre_audio.dtypes if t[1] == 'int' or t[1] == 'double']
#Q2 d)
#####################################################################################################
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.feature import IndexToString,StringIndexer, VectorAssembler
from pyspark.ml.evaluation import MulticlassClassificationEvaluator 
from pyspark.mllib.evaluation import MulticlassMetrics
from pyspark.ml import Pipeline
#convert a string column to a label column
labelIndexer = StringIndexer(inputCol="genre", outputCol="label").fit(electronic_genre)
#merging multiple columns into a vector column
vecAssembler = VectorAssembler(inputCols=numeric_features, outputCol="features")

(trainingData, testData) = electronic_genre.randomSplit([0.8, 0.2], seed = 100)

#get class balance
df_class_0 = trainingData[trainingData['genre'] == "Other"]
df_class_1 = trainingData[trainingData['genre'] == "Electronic"]

#create dictionary of fraction of each class
fractions=dict()
fractions["Other"] = min(df_class_0.count(),df_class_1.count())/df_class_0.count()
fractions["Electronic"] = min(df_class_0.count(),df_class_1.count())/df_class_1.count()

#down sampling training using functions
trainingData = trainingData.sampleBy("genre",fractions,seed=1)

# By using down sampling we could see an increase in precision while there is a decrease in accuracy and recall

# Spliting the data using stratified sampling
#from pyspark.sql.functions import lit
#fractions = electronic_genre.select("genre").distinct().withColumn("fraction", lit(0.8)).rdd.collectAsMap()
#trainingData = electronic_genre.stat.sampleBy("genre", fractions, seed=20)
#testData = electronic_genre.subtract(trainingData)

# Train a RandomForest model.
rf = RandomForestClassifier(labelCol="label", featuresCol="features", numTrees=10)
# Convert indexed labels back to original labels.
labelConverter = IndexToString(inputCol="prediction", outputCol="predictedLabel",
                               labels=labelIndexer.labels)
# Chain labelIndexer, vecAssembler and NBmodel in a 
pipeline = Pipeline(stages=[labelIndexer, vecAssembler, rf,labelConverter])
# Run stages in pipeline and train model
model = pipeline.fit(trainingData)
#Make Predictions
predictions = model.transform(testData)
# Select example rows to display.
result = predictions.select("prediction", "label")
predictionAndLabels = result.rdd
metrics = MulticlassMetrics(predictionAndLabels)
cm=metrics.confusionMatrix().toArray()
accuracy=(cm[0][0]+cm[1][1])/cm.sum()
precision=(cm[0][0])/(cm[0][0]+cm[1][0])
recall=(cm[0][0])/(cm[0][0]+cm[0][1])
print("RandomForestClassifier: accuracy,precision,recall",accuracy,precision,recall)
# Select (prediction, true label) and compute test error
evaluator = MulticlassClassificationEvaluator(
    labelCol="label", predictionCol="prediction", metricName="accuracy")
accuracy = evaluator.evaluate(predictions)
print("Test Error = %g" % (1.0 - accuracy))
rfModel = model.stages[2]
print(rfModel)  # summary only
#Cross - Validation
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator
paramGrid = (ParamGridBuilder()
             .addGrid(rf.maxDepth, [2, 4, 6])
             .addGrid(rf.maxBins, [20, 60])
             .addGrid(rf.numTrees, [3, 10])
             .build())
#Perform Cross-validation
cv = CrossValidator(estimator=pipeline, estimatorParamMaps=paramGrid, evaluator=evaluator, numFolds=5)
# Run cross validations.  
cvModel = cv.fit(trainingData)
predictions = cvModel.transform(testData)
evaluator.evaluate(predictions)
###################################################################################################################

from pyspark.ml.classification import DecisionTreeClassifier
#convert a string column to a label column
labelIndexer = StringIndexer(inputCol="genre", outputCol="label").fit(electronic_genre)
#merging multiple columns into a vector column
vecAssembler = VectorAssembler(inputCols=numeric_features, outputCol="features")
# Randomly splitting the data
#(trainingData, testData) = electronic_genre.randomSplit([0.9, 0.1], seed = 100)

# Spliting the data using stratified sampling
from pyspark.sql.functions import lit
fractions = electronic_genre.select("genre").distinct().withColumn("fraction", lit(0.8)).rdd.collectAsMap()
trainingData = electronic_genre.stat.sampleBy("genre", fractions, seed=20)
testData = electronic_genre.subtract(trainingData)

# Train a DecisionTree model.
dt = DecisionTreeClassifier(labelCol="label", featuresCol="features")
# Chain indexers and tree in a Pipeline
pipeline = Pipeline(stages=[labelIndexer, vecAssembler, dt])
# Train model.  This also runs the indexers.
model = pipeline.fit(trainingData)
# Make predictions.
predictions = model.transform(testData)
# Select example rows to display.
result = predictions.select("prediction", "label")
predictionAndLabels = result.rdd
metrics = MulticlassMetrics(predictionAndLabels)
cm=metrics.confusionMatrix().toArray()
accuracy=(cm[0][0]+cm[1][1])/cm.sum()
precision=(cm[0][0])/(cm[0][0]+cm[1][0])
recall=(cm[0][0])/(cm[0][0]+cm[0][1])
print("DecisionTreeClassifier: accuracy,precision,recall",accuracy,precision,recall)
# Select (prediction, true label) and compute test error
evaluator = MulticlassClassificationEvaluator(
    labelCol="label", predictionCol="prediction", metricName="accuracy")
accuracy = evaluator.evaluate(predictions)
print("Test Error = %g " % (1.0 - accuracy))
treeModel = model.stages[2]
# summary only
print(treeModel)
model.stages[2]._call_java('toDebugString')
#Cross - Validation
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator
paramGrid = (ParamGridBuilder()
             .addGrid(dt.maxDepth, [2, 4, 6, 8])
             .addGrid(dt.maxBins, [20, 60])
             .build())
#Perform Cross-validation
cv = CrossValidator(estimator=pipeline, estimatorParamMaps=paramGrid, evaluator=evaluator, numFolds=5)
# Run cross validations.  
cvModel = cv.fit(trainingData)
predictions = cvModel.transform(testData)
evaluator.evaluate(predictions)

###################################################################################################################

from pyspark.ml.classification import GBTClassifier
#convert a string column to a label column
labelIndexer = StringIndexer(inputCol="genre", outputCol="label").fit(electronic_genre)
#merging multiple columns into a vector column
vecAssembler = VectorAssembler(inputCols=numeric_features, outputCol="features")
# Randomly splitting the data
(trainingData, testData) = electronic_genre.randomSplit([0.8, 0.2], seed = 100)

# Spliting the data using stratified sampling
#from pyspark.sql.functions import lit
#fractions = electronic_genre.select("genre").distinct().withColumn("fraction", lit(0.8)).rdd.collectAsMap()
#trainingData = electronic_genre.stat.sampleBy("genre", fractions, seed=20)
#testData = electronic_genre.subtract(trainingData)

#from sklearn.model_selection import train_test_split
#from imblearn.over_sampling import SMOTE
#training,test = train_test_split(electronic_genre,test_size = .2,random_state=12)
#sm = SMOTE(random_state=12, ratio = 1.0)
#trainingData,testData = sm.fit_sample(training, test)
# Train a GBT model
gbt = GBTClassifier(labelCol="label", featuresCol="features",maxIter=10)
pipeline = Pipeline(stages=[labelIndexer, vecAssembler, gbt])
gbtModel = pipeline.fit(trainingData)
# Make predictions.
predictions = gbtModel.transform(testData)
# Select example rows to display.
result = predictions.select("prediction", "label")
predictionAndLabels = result.rdd
metrics = MulticlassMetrics(predictionAndLabels)
cm=metrics.confusionMatrix().toArray()
accuracy=(cm[0][0]+cm[1][1])/cm.sum()
precision=(cm[0][0])/(cm[0][0]+cm[1][0])
recall=(cm[0][0])/(cm[0][0]+cm[0][1])
print("DecisionTreeClassifier: accuracy,precision,recall",accuracy,precision,recall)
# Select (prediction, true label) and compute test error
evaluator = MulticlassClassificationEvaluator(
    labelCol="label", predictionCol="prediction", metricName="accuracy")
accuracy = evaluator.evaluate(predictions)
print("Test Error = %g " % (1.0 - accuracy))
#Q2 f)
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator
paramGrid = (ParamGridBuilder()
             .addGrid(gbt.maxDepth, [2, 4, 6])
             .addGrid(gbt.maxBins, [20, 60])
             .addGrid(gbt.maxIter, [10, 20])
             .build())
#Perform Cross-validation
cv = CrossValidator(estimator=pipeline, estimatorParamMaps=paramGrid, evaluator=evaluator, numFolds=5)
# Run cross validations.  This can take about 6 minutes since it is training over 20 trees!
cvModel = cv.fit(trainingData)
predictions = cvModel.transform(testData)
evaluator.evaluate(predictions)

#Q3 a) (working)

#One vs Rest is a type of machine learning reduction algorith that uses a base classifier model which work for a single class
#for all the remaining class for attaining the multiclass output.

#The one vs one classification technique, as the name suggests, is about picking a pair of classes from a set of 𝑛 classes 
#and develop a binary classifier for each pair. So given 𝑛 classes we can pick all possible combinations of pairs of classes 
#from 𝑛 and then for each pair we develop a binary support vector machine (SVM).The winning class is picked as winner.

#OneVsRest
from pyspark.ml.classification import LogisticRegression, OneVsRest
#convert a string column to a label column
label_stringIdx = StringIndexer(inputCol = "genre", outputCol = "label").fit(genre_audio)
#merging multiple columns into a vector column
vecAssembler = VectorAssembler(inputCols=numeric_features, outputCol="features")
# Spliting the data using stratified sampling
from pyspark.sql.functions import lit
fractions = genre_audio.select("genre").distinct().withColumn("fraction", lit(0.8)).rdd.collectAsMap()
trainingData = genre_audio.stat.sampleBy("genre", fractions, seed=20)
testData = genre_audio.subtract(trainingData)
# instantiate the base classifier.
lr = LogisticRegression(maxIter=10, tol=1E-6, fitIntercept=True)
# instantiate the One Vs Rest Classifier.
ovr = OneVsRest(classifier=lr)
pipeline = Pipeline(stages=[label_stringIdx,vecAssembler,ovr])
# train the multiclass model.
ovrModel = pipeline.fit(trainingData)
# score the model on test data.
predictions = ovrModel.transform(testData)
# Select example rows to display.
predictions.select("prediction", "label", "features").show(5)
# Select (prediction, true label) and compute test error
evaluator = MulticlassClassificationEvaluator(
    labelCol="label", predictionCol="prediction", metricName="accuracy")
accuracy = evaluator.evaluate(predictions)
print("Test Error = %g " % (1.0 - accuracy))

#########################################################################################################################
#Multiclass classification
from pyspark.sql.functions import rand
#convert a string column to a label column
label_stringIdx = StringIndexer(inputCol = "genre", outputCol = "label")
#merging multiple columns into a vector column
vecAssembler = VectorAssembler(inputCols=numeric_features, outputCol="features")

pipeline = Pipeline(stages=[label_stringIdx,vecAssembler])
# Fit the pipeline to training documents.
pipelineFit = pipeline.fit(genre_audio)
dataset = pipelineFit.transform(genre_audio)
dataset.show(5)
# Spliting the data using stratified sampling
from pyspark.sql.functions import lit
fractions = dataset.select("genre").distinct().withColumn("fraction", lit(0.8)).rdd.collectAsMap()
trainingData = dataset.stat.sampleBy("genre", fractions, seed=20)
testData = dataset.subtract(trainingData)
# instantiate the base classifier.
rf = RandomForestClassifier(labelCol="label", featuresCol="features", numTrees=12,  maxDepth=10)
#Train the model
rfModel = rf.fit(trainingData)
#Make predictions
predictions = rfModel.transform(testData)

predictions.filter(predictions['prediction'] == 0) \
    .select("probability","label","prediction") \
    .orderBy("probability", ascending=False) \
    .show(n = 10, truncate = 30)

evaluator = MulticlassClassificationEvaluator(predictionCol="prediction")
evaluator.evaluate(predictions)
# Select (prediction, true label) and compute test error
evaluator = MulticlassClassificationEvaluator(
    labelCol="label", predictionCol="prediction", metricName="accuracy")
accuracy = evaluator.evaluate(predictions)
print("Test Error = %g " % (1.0 - accuracy))

#Song Recommendations
#Q1 a)
triplets.groupby("song_id").agg({'user_id': 'count'}).count()
#There are totally 378310 unique songs
triplets.groupby("user_id").agg(F.countDistinct("song_id")).count()
#There are totally 1019318 unique users
#b)
play_count = (
    triplets
    .select(["user_id","song_id","plays"])
    .groupby("user_id")
    .agg({'plays': 'sum'})
    .orderBy('sum(plays)', ascending=False)
    .withColumnRenamed('sum(plays)', 'play_count')
    .drop('play_count')
    )
play_count_max = play_count.first()
# The most active user id is '093cb74eb3c517c5179ae24caf0ebec51b24d2a2'
unique_songs = (
    triplets
    .select(["user_id","song_id","plays"])
    .groupby("user_id","song_id")
    .agg({'plays': 'count'})
    .orderBy('count(plays)', ascending=False)
    .withColumnRenamed('count(plays)', 'unique_songs')
    )
unique_songs.filter(unique_songs["user_id"] == play_count_max["user_id"]).count()
#The most active user has played 195 different songs
#This count is just 0.051545% of the unique songs in the entire dataset
#c)

import matplotlib.pyplot as plt
songs_count = (
    triplets
    .select(["user_id","song_id","plays"])
    .groupby("song_id")
    .agg({'plays': 'sum'})
    .orderBy('sum(plays)', ascending=False)
    .withColumnRenamed('sum(plays)', 'play_count')
    )
songs_count = songs_count.filter(songs_count.play_count <= 50000).filter(songs_count.play_count >= 500)
songs_count.show(10,False)

# Figure
from pyspark_dist_explore import hist
f, a = plt.subplots(dpi=300, figsize=(10, 5))
bins, counts = songs_count.select('play_count').rdd.flatMap(lambda x: x).histogram(50)
# This is a bit awkward but I believe this is the correct way to do it 
plt.hist(bins[:-1], bins=bins, weights=counts)
plt.xlabel('Plays')
plt.ylabel('Number of Songs')
plt.title('Songs Popularity')
# Save
plt.tight_layout()  # reduce whitespace
f.savefig(os.path.join(output_path, f"songs_count.png"), bbox_inches="tight")  # save as png and view in windows
plt.close(f)
###################################################################################################################

users_count = (
    triplets
    .select(["user_id","song_id","plays"])
    .groupby("user_id")
    .agg({'plays': 'sum'})
    .orderBy('sum(plays)', ascending=False)
    .withColumnRenamed('sum(plays)', 'play_count')
    )
users_count = users_count.filter(users_count.play_count <= 2000).filter(users_count.play_count >= 10)
users_count.show(10,False)

# Figure
f, a = plt.subplots(dpi=300, figsize=(10, 5))  # affects output resolution (dpi) and font scaling (figsize)
# Show histogram of the 'C1' column
bins, counts = users_count.select('play_count').rdd.flatMap(lambda x: x).histogram(50)
# This is a bit awkward but I believe this is the correct way to do it 
plt.hist(bins[:-1], bins=bins, weights=counts)
# Labels
plt.title(f"User Activity")
plt.xlabel("Plays")
plt.ylabel("Number of Users")
# Save
plt.tight_layout()  # reduce whitespace
f.savefig(os.path.join(output_path, f"users_count.png"), bbox_inches="tight")  # save as png and view in windows
plt.close(f)
###################################################################################################################
#Q2 a)
from pyspark.ml.recommendation import ALS
from pyspark.ml.evaluation import RegressionEvaluator
userIndexer = StringIndexer(inputCol="user_id", outputCol="new_user_id").fit(triplets)
songIndexer = StringIndexer(inputCol="song_id", outputCol="new_song_id").fit(triplets)
pipeline=Pipeline(stages=[userIndexer,songIndexer])
indexedTriplets = pipeline.fit(triplets).transform(triplets)
# join dataframes
users_count = users_count.drop("play_count")
songs_count = songs_count.drop("play_count")
indexedTriplets = indexedTriplets.join(users_count, 'user_id').join(songs_count, 'song_id')
triplets_metadata = indexedTriplets.join(metadata_summary,on= "song_id",how = "inner").drop(metadata_summary.song_id).distinct()
indexedTriplets = indexedTriplets.withColumn("plays", indexedTriplets["plays"].cast(DoubleType()))
# We'll hold out 60% for training, 20% of our data for validation, and leave 20% for testing
(trainingData,validationData,testData) = indexedTriplets.randomSplit([0.6,0.2,0.2])
indexedTriplets.cache()
trainingData.cache()
validationData.cache()
reg_eval = RegressionEvaluator(predictionCol="prediction", labelCol="plays", metricName="rmse")
regParams = [0.25]
ranks = [16]
tolerance = 0.03
errors = [[0]*len(ranks)]*len(regParams)
models = [[0]*len(ranks)]*len(regParams)
err = 0
min_error = float('inf')
best_rank = -1
i=0
for regParam in regParams:
	j=0
	for rank in ranks:
		als = ALS(maxIter=5, regParam=regParam,rank= rank,alpha=80, seed=8427,userCol="new_user_id", 
			itemCol="new_song_id", ratingCol="plays",implicitPrefs=True)
		model = als.fit(trainingData)
		# Evaluate the model by computing the RMSE on the test data
		predictions = model.transform(validationData)
		# Remove NaN values from prediction (due to SPARK-14489)
		predicted_plays_df = predictions.filter(predictions.prediction != float('nan'))
		#evaluator = RegressionEvaluator(metricName="rmse", labelCol="plays",predictionCol="prediction")
		#rmse = evaluator.evaluate(predictions)
		#print("For regParam: " + str(regParam) + ", rank: " +str(rank) + ", alpha: " + str(alpha) + ", Root-mean-square error = " + str(rmse))
		# Run the previously created RMSE evaluator, reg_eval, on the predicted_ratings_df DataFrame
		error = reg_eval.evaluate(predicted_plays_df)
		errors[i][j] = error
		models[i][j] = model
		print("For rank " + str(rank) + ", regularization parameter " + str(regParam) + "the RMSE is " + str(error))
		if error < min_error:
			min_error = error
			best_params = [i,j]
		j += 1
	i += 1
#Setting the Best parameters	
als.setRegParam(regParams[best_params[0]])
als.setRank(ranks[best_params[1]])
print("The best model was trained with regularization parameter " + str(regParams[best_params[0]]))
print("The best model was trained with rank " + str(ranks[best_params[1]]))
my_model = models[best_params[0]][best_params[1]]
#Testing the Model
predictions = my_model.transform(testData)
# Remove NaN values from prediction (due to SPARK-14489)
predicted_test_df = predictions.filter(predictions.prediction != float('nan'))
predicted_test_df = predicted_test_df.withColumn("prediction", F.abs(F.round(predicted_test_df["prediction"],0)))
test_RMSE = reg_eval.evaluate(predicted_test_df)
print('The model had a RMSE on the test set of {0}'.format(test_RMSE))

# Generate top 10 song recommendations for a specified set of users
# Create original DataFrame users
users = sqlContext.createDataFrame([[219503.0],[681577.0],[345773.0],[337178.0],[717504.0]], ["new_user_id"])
#users = indexedTriplets.select(als.getUserCol()).distinct().limit(5)
users.show(5,False)
songSubSetRecs = my_model.recommendForUserSubset(users, 10)
songSubSetRecs = songSubSetRecs.withColumn("songAndPlays", F.explode(songSubSetRecs.recommendations)).select("new_user_id","songAndPlays.*")
triplets_metadata.registerTempTable('triplets_metadata_tbl')
songSubSetRecs.registerTempTable('songSubSetRecs_tbl')
songSubSetRecs_sql = """ SELECT songSubSetRecs_tbl.*,triplets_metadata_tbl.artist_name,triplets_metadata_tbl.title 
from songSubSetRecs_tbl INNER JOIN triplets_metadata_tbl ON triplets_metadata_tbl.new_song_id = songSubSetRecs_tbl.new_song_id"""
songSubSetReccomendations = spark.sql(songSubSetRecs_sql)
songSubSetReccomendations = songSubSetReccomendations.distinct().orderBy(["new_user_id","rating"],ascending=False)
songSubSetReccomendations.show(50,False)
#Actual Play for the same users
users.registerTempTable('users_tbl')
song_actual_sql = """ SELECT users_tbl.*,triplets_metadata_tbl.new_song_id,triplets_metadata_tbl.artist_name,triplets_metadata_tbl.title,
triplets_metadata_tbl.plays from users_tbl INNER JOIN triplets_metadata_tbl ON triplets_metadata_tbl.new_user_id = users_tbl.new_user_id""" 
song_actual = spark.sql(song_actual_sql)
song_actual = song_actual.distinct().orderBy(["new_user_id","plays"],ascending=False)
song_actual.show(150,False)
#Matches between Actual and Predicted
songSubSetReccomendations.registerTempTable('songSubSetReccomendations_tbl')
song_actual.registerTempTable('song_actual_tbl')
new_sql = """ SELECT songSubSetReccomendations_tbl.*,song_actual_tbl.plays from songSubSetReccomendations_tbl INNER JOIN 
song_actual_tbl ON songSubSetReccomendations_tbl.new_user_id = song_actual_tbl.new_user_id AND 
songSubSetReccomendations_tbl.title = song_actual_tbl.title"""
new = spark.sql(new_sql)
new.show(150,False)

#Calculating the Metrics
songSubSetRecs_rdd = songSubSetRecs.groupby("new_user_id").agg(F.collect_list("new_song_id").alias("recommended"))
song_actual_rdd = song_actual.groupby("new_user_id").agg(F.collect_list("new_song_id").alias("actual"))
song_eval_rdd = songSubSetRecs_rdd.join(song_actual_rdd,'new_user_id')
song_eval_rdd = song_eval_rdd.drop("new_user_id").rdd
from pyspark.mllib.evaluation import RankingMetrics
evaluator_metric = RankingMetrics(song_eval_rdd)
evaluator_metric.precisionAt(5)         #Precision @5
evaluator_metric.ndcgAt(10)             #NDCG @10
evaluator_metric.meanAveragePrecision   #Mean Average Precision

#Stop the current Spark Session
spark.sparkContext.stop()

#Extra
#song_plays_grouped = triplets_int_ids.groupBy("song_id").count().show()
#songs_count.groupby('play_count').count().select('count').rdd.flatMap(lambda x: x).histogram(20)
#triplets_int_ids.filter(triplets_int_ids['new_song_id']==47026)
#predictions.groupBy("prediction").count().show()
#metadata_summary.filter(metadata_summary["title"].rlike("Super")).show()




