# MarkdownSparkSQL
Markdown repo with Scala-SparkSQL examples ```codigo```

## Jupyter configuration for show outputs
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
val parquet_file = spark.read.parquet("full_path_to_parquet_partitions")
```
    * Example:
```scala
val parquet_table = spark.read.parquet("/datos/parquet_files/weather_info")
```