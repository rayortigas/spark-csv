/*
 * Copyright 2014 Databricks
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.databricks.spark

import org.apache.commons.csv.CSVFormat
import org.apache.hadoop.io.compress.CompressionCodec

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SQLContext, DataFrame, Row}
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types.StructType

package object csv {

  /**
   * Adds a method, `csvFile`, to SQLContext that allows reading CSV data.
   */
  implicit class CsvContext(sqlContext: SQLContext) {
    def csvFile(filePath: String,
                useHeader: Boolean = true,
                delimiter: Char = ',',
                quote: Char = '"',
                escape: Char = '\\',
                mode: String = "PERMISSIVE") = {
      val csvRelation = CsvRelation(
        location = filePath,
        useHeader = useHeader,
        delimiter = delimiter,
        quote = quote,
        escape = escape,
        parseMode = mode)(sqlContext)
      sqlContext.baseRelationToDataFrame(csvRelation)
    }

    def tsvFile(filePath: String, useHeader: Boolean = true) = {
      val csvRelation = CsvRelation(
        location = filePath,
        useHeader = useHeader,
        delimiter = '\t',
        quote = '"',
        escape = '\\',
        parseMode = "PERMISSIVE")(sqlContext)
      sqlContext.baseRelationToDataFrame(csvRelation)
    }
  }
  
  implicit class CsvContextRDD(sqlContext: SQLContext) {
    def csvFileToRDD[T](
      filePath: String,
      useHeader: Boolean = true,
      delimiter: Char = ',',
      quote: Char = '"',
      escape: Char = '\\',
      mode: String = "PERMISSIVE")(implicit manifest: Manifest[T]): RDD[T] = {
      
      val schema = ScalaReflection.schemaFor[T].dataType.asInstanceOf[StructType]
      val csvRelation = CsvRelation(
        location = filePath,
        useHeader = useHeader,
        delimiter = delimiter,
        quote = quote,
        escape = escape,
        parseMode = mode,
        userSchema = schema)(sqlContext)
      val df = sqlContext.baseRelationToDataFrame(csvRelation)
      df.mapPartitions { iter =>
        val rowConverter = RowConverter[T]()
        iter.map { row => rowConverter.convert(row) }
      }
    }
  }
  
  case class RowConverter[T](implicit manifest: Manifest[T]) {
    // http://docs.scala-lang.org/overviews/reflection/environment-universes-mirrors.html#types-of-mirrors-their-use-cases--examples
    val ru = scala.reflect.runtime.universe
    val m = ru.runtimeMirror(getClass.getClassLoader)
    val classT = ru.typeOf[T].typeSymbol.asClass
    val cm = m.reflectClass(classT)
    val ctorT = ru.typeOf[T].declaration(ru.nme.CONSTRUCTOR).asMethod
    val ctorm = cm.reflectConstructor(ctorT)
  
    def convert(row: Row): T = {
      val args = row.toSeq
      require(ctorT.paramss.head.size == args.size)
      ctorm.apply(row.toSeq: _*).asInstanceOf[T]
    }
  }
  
  implicit class CsvSchemaRDD(dataFrame: DataFrame) {

    /**
     * Saves DataFrame as csv files. By default uses ',' as delimiter, and includes header line.
     */
    def saveAsCsvFile(path: String, parameters: Map[String, String] = Map(),
                      compressionCodec: Class[_ <: CompressionCodec] = null): Unit = {
      // TODO(hossein): For nested types, we may want to perform special work
      val delimiter = parameters.getOrElse("delimiter", ",")
      val delimiterChar = if (delimiter.length == 1) {
        delimiter.charAt(0)
      } else {
        throw new Exception("Delimiter cannot be more than one character.")
      }

      val escape = parameters.getOrElse("escape", "\\")
      val escapeChar = if (escape.length == 1) {
        escape.charAt(0)
      } else {
        throw new Exception("Escape character cannot be more than one character.")
      }

      val quoteChar = parameters.get("quote") match {
        case Some(s) => {
          if (s.length == 1) {
            Some(s.charAt(0))
          } else {
            throw new Exception("Quotation cannot be more than one character.")
          }
        }
        case None => None
      }

      val generateHeader = parameters.getOrElse("header", "false").toBoolean
      val header = if (generateHeader) {
        dataFrame.columns.map(c => s""""$c"""").mkString(delimiter)
      } else {
        "" // There is no need to generate header in this case
      }
      val strRDD = dataFrame.rdd.mapPartitions { iter =>
        val csvFormatBase = CSVFormat.DEFAULT
          .withDelimiter(delimiterChar)
          .withEscape(escapeChar)
          .withSkipHeaderRecord(false)
          .withNullString("null")

        val csvFormat = quoteChar match {
          case Some(c) => csvFormatBase.withQuote(c)
          case _ => csvFormatBase
        }

        new Iterator[String] {
          var firstRow: Boolean = generateHeader

          override def hasNext = iter.hasNext

          override def next: String = {
            val row = csvFormat.format(iter.next.toSeq.map(_.asInstanceOf[AnyRef]):_*)
            if (firstRow) {
              firstRow = false
              header + "\n" + row
            } else {
              row
            }
          }
        }
      }
      compressionCodec match {
        case null => strRDD.saveAsTextFile(path)
        case codec => strRDD.saveAsTextFile(path, codec)
      }
    }
  }
}
