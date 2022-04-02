import findspark
findspark.init()
import pyspark.sql.functions as F
from pyspark import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql import dataframe

sc = SparkContext("local", "applying-apis")
spark = SparkSession(sc)



def read_csv(spark: SparkSession, file_path: str):
    """
    Reads a CSV file with the path `file_path` into a dataframe
    using the spark session `spark`.
    """
    return spark.read.option("inferSchema", "true").option("header", "true").csv(file_path)


def write_csv(df: dataframe, folder_path: str):
    """
    Writes the dataframe `df` into a folder with the path `folder_path`.
    """
    df.write.format("csv").option(
        "header", "true").save(folder_path)


df = read_csv(spark, 'data_Novetta_CLAVIN.csv')

abstractionLevels = ['package', 'class', 'method']

cols = ['packageName', 'className', 'methodName', 'api', 'mcrCategories', 'mcrTags']
dfselected = df.where(df.isAPIClass == True).select(cols)

apilist = dfselected.select('api').distinct().collect()

analysiscolumns = ['API_A', 'API_B', 'MethodAbstraction', 'ClassAbstraction', 'PackageAbstraction']

vals = []

print('Total APIs: ', len(apilist))

for A in range(len(apilist)):
    for B in range(A + 1, len(apilist)):
        print('pair: ' + apilist[A].api + ' , ' + apilist[B].api)

        # MethodLevel
        methodA = dfselected.filter(df.api == apilist[A].api).filter(df.methodName.isNotNull()).select(
            'methodName').distinct()
        methodB = dfselected.filter(df.api == apilist[B].api).filter(df.methodName.isNotNull()).select(
            'methodName').distinct()
        methodInter = methodA.intersect(methodB)
        mixedInstancesMethod = methodInter.distinct().count()

        # ClassLevel
        classA = dfselected.filter(df.api == apilist[A].api).filter(df.className.isNotNull()).select(
            'className').distinct()
        classB = dfselected.filter(df.api == apilist[B].api).filter(df.className.isNotNull()).select(
            'className').distinct()
        classInter = classA.intersect(classB)
        mixedInstancesClass = classInter.distinct().count()

        # PackageLevel
        packageA = dfselected.filter(df.api == apilist[A].api).filter(df.packageName.isNotNull()).select(
            'packageName').distinct()
        packageB = dfselected.filter(df.api == apilist[B].api).filter(df.packageName.isNotNull()).select(
            'packageName').distinct()
        packageInter = packageA.intersect(packageB)
        mixedInstancesPackage = packageInter.distinct().count()

        vals.append((apilist[A].api, apilist[B].api, mixedInstancesMethod, mixedInstancesClass, mixedInstancesPackage))

dfResult = spark.createDataFrame(vals, analysiscolumns)

dfResult.show(200)
write_csv(dfResult, 'Result')


