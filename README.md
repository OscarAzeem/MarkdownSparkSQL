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