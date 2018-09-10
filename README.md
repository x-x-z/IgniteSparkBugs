# Apache Ignite Spark Bugs

This project demonstrates the bugs found in the current version of Apache Spark Ignite 2.6

## Bugs description

1. org.apache.spark.sql.Dataset#union(org.apache.spark.sql.Dataset) failed with org.apache.ignite.spark.IgniteDataFrameSettings#OPTION_DISABLE_SPARK_SQL_OPTIMIZATION() = false (default)
	 
Unit Test source code:
```
org.xxz.ignite_spark_bugs.IgniteSparkBugsApplicationTests#unionDatasetsTest
```

2. org.apache.ignite.spark.impl.optimization.SimpleExpressions$#toString does not check String for null

Unit Test source code:
```
org.xxz.ignite_spark_bugs.IgniteSparkBugsApplicationTests#showTableWithNullValuesTest
```

3. org.apache.ignite.spark.impl.IgniteSQLRelation$.schema does not use the org.apache.ignite.cache.QueryEntity.aliases that the user can specify using the annotation org.apache.ignite.cache.query.annotations.QuerySqlField
	
Unit Test source code:
```
org.xxz.ignite_spark_bugs.IgniteSparkBugsApplicationTests#readTableWithQuerySqlFieldTest
```