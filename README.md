# MarkdownSparkSQL
Markdown repo with ```Scala-SparkSQL``` examples

## Jupyter configuration for showing outputs
```html
%%html
<style>
.output_subarea.output_text.output_stream.output_stdout > pre {
  width:max-content;
}
.p-Widget.jp-RenderedText.jp-OutputArea-output > pre {
  width:max-content;
}
</style>
```

## Scala libraries
```scala
import org.apache.spark.sql.SparkSession
val sqlContext = new org.apache.spark.sql.SQLContext(sc)
import org.apache.spark.sql.SparkSession.{ _}
import org.apache.spark.sql.functions.{col, _}
import org.apache.spark.sql.types.{ _}
import org.apache.spark.sql.{ _}
import org.joda.time.{ _}
import org.apache.spark.sql.expressions.Window
```

## Code
* Unwrap a List inside a function
```scala
val list_to_unwrap = List("a","b")
function(list_to_unwrap:_*)
```
* Read a parquet file to a Spark Dataframe
```scala
val parquet_file = spark.read.parquet("/datos/parquet_files/weather_info")
.select(col("*"))
```
* Show with orderBy
```scala
parquet_file.orderBy(col("column_name").desc, col("column_name2").desc).show(10,false)
```
* Filter a dataframe, multiple conditions
```scala
val dataframe=parquet_file
.filter(col("column_name")
&& col("column_name2")==="A"
&& col("column_name_date")<="2021-01-31"
&& col("column_name_date")>="2021-01-01"
&& col("column_name_amount")=!="0"
&& (col("column_name3")==="C" || col("column_name4")==="E")
&& col("column_ids").isin(list_to_unwrap:_*)
)
.select(col("column_name").alias("alias_column_name")
,col("column_name2"))
```
* String Expressions
    * Concat and substring
    ```scala
    val dataframe=parquet_file.select(
    concat(lit("00"),substring(col("column_name1"),-10,10)).alias("column_name_alias"),
    )
    ```
* Group by expression
    * count
    ```scala
    val dataframe = parquet_file
    .filter(
        col("column_name1").isNotNull
        && col("column_name2").isNotNull
    )
    .groupBy(
    col("column_name3"),
    col("column_name2"),
    col("column_name4"),
    col("column_name1"))
    .count
    ```
    * aggregator (sum)
    ```scala
    val dataframe=parquet_file
    .filter(col("column_name1").isNotNull)
    .groupBy(
    col("column_name1"),
    col("column_name2"),
    col("column_name3")
    ).agg(sum(col("column_name_amount")).alias("column_name_total"))
    ```

* Window functions
```scala
val parquet_file2 = parquet_file
.select(col("column_name1")
       ,col("column_name2"))
.withColumn("max_date",row_number()
            .over(Window.partitionBy(col("column_date"),col("partition_column"))
            .orderBy(col("column_date").desc)))
            .filter(col("max_date")===1)
.withColumn("max_date",row_number()
            .over(Window.partitionBy(col("partition_column"))
            .orderBy(col("column_date").desc)))
            .filter(col("max_date")===1)
```
* Select case when
```scala
val parquet_file2=parquet_file
.select(
col("column_name1")
,col("column_name2")
,col("column_name3").alias("columna_name_alias")
,when(col("column_name1")<"0" 
    && col("column_name2").isNotNull
    && col("column_name3").isNull,
    col("column_output_when")*lit(-1)
)
    .otherwise(col("column_name6")
)
.alias("column_name_alias_when")
)
```
* Join example
```scala
val parquet_df=parquet_file1.join(parquet_file2,
    parquet_file1.col("column_name_id")===parquet_file2.col("column_name_id")
    && parquet_file1.col("column_name_id2")===parquet_file2.col("column_name_id2")
    ,"inner") //left, right, inner
.select(
pqrquet_file1.col("column_name1")
,pqrquet_file1.col("column_name2")
,parquet_file2.col("column_name1").alias("column_name_alias")
,when(parquet_file2.col("column_name_amount")<"0" 
    && parquet_file1.col("column_name3").isNotNull
)
    .otherwise(parquet_file2.col("column_name2")
)
.alias("column_name_alias")
,parquet_file2.col("column_name6")
)
```
