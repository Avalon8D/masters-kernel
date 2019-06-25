package linalg_impl

import org.apache.spark.sql.expressions.MutableAggregationBuffer
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DataType, LongType, DoubleType, ArrayType, StructType, StructField}

class VecsSum (val dim:Int) extends UserDefinedAggregateFunction {
    // This is the input fields for your aggregate function.
    override def inputSchema: org.apache.spark.sql.types.StructType =
        StructType(
            StructField("count", LongType) ::
            StructField("arr", ArrayType (DoubleType)) :: Nil
        )

    // This is the internal fields you keep for computing your aggregate.
    override def bufferSchema: StructType = StructType(
        StructType(StructField("sum", ArrayType (DoubleType)) :: Nil)
    )

    // This is the output type of your aggregatation function.
    override def dataType: DataType = ArrayType (DoubleType)

    override def deterministic: Boolean = true

    // This is the initial value for your buffer schema.
    override def initialize(buffer: MutableAggregationBuffer): Unit = {
        buffer(0) = (0 until dim).map {case a => 0.0}.toSeq
    }

    // This is how to update your buffer schema given an input.
    override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
        val cols = input.getLong(0).toInt
        val Y = input.getSeq[Double](1)
        val Y_dense = new breeze.linalg.DenseMatrix (
            Y.length / cols, cols,
            Y.toArray
        )

        val dense_buffer = new breeze.linalg.DenseVector (buffer.getSeq[Double](0).toArray)

        // plus column sums
        dense_buffer += breeze.linalg.sum (Y_dense (breeze.linalg.*, ::))

        buffer(0) = dense_buffer.data.toSeq
    }

    // This is how to merge two objects with the bufferSchema type.
    override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
        val dense_buffer1 = new breeze.linalg.DenseVector (buffer1.getSeq[Double](0).toArray)
        val dense_buffer2 = new breeze.linalg.DenseVector (buffer2.getSeq[Double](0).toArray)
        
        dense_buffer1 += dense_buffer2

        buffer1(0) = dense_buffer1.data.toSeq
    }

    // This is where you output the final value, given the final value of your bufferSchema.
    override def evaluate(buffer: Row): Any = {
        buffer.getSeq[Double](0)
    }
}

class VecsMean (val dim:Int) extends UserDefinedAggregateFunction {
    // This is the input fields for your aggregate function.
    override def inputSchema: org.apache.spark.sql.types.StructType =
        StructType(
            StructField("count", LongType) ::
            StructField("arr", ArrayType (DoubleType)) :: Nil
        )

    // This is the internal fields you keep for computing your aggregate.
    override def bufferSchema: StructType = StructType(
        StructType(
            StructField("sum", ArrayType (DoubleType)) :: 
            StructField("count", LongType) :: Nil
        )
    )

    // This is the output type of your aggregatation function.
    override def dataType: DataType = ArrayType (DoubleType)

    override def deterministic: Boolean = true

    // This is the initial value for your buffer schema.
    override def initialize(buffer: MutableAggregationBuffer): Unit = {
        buffer(0) = (0 until dim).map {case a => 0.0}.toSeq
        buffer(1) = 0.toLong
    }

    // This is how to update your buffer schema given an input.
    override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
        val cols = input.getLong(0).toInt
        val Y = input.getSeq[Double](1)
        val Y_dense = new breeze.linalg.DenseMatrix (
            Y.length / cols, cols,
            Y.toArray
        )

        val dense_buffer = new breeze.linalg.DenseVector (buffer.getSeq[Double](0).toArray)

        // plus column sums
        dense_buffer += breeze.linalg.sum (Y_dense (breeze.linalg.*, ::))

        buffer(0) = dense_buffer.data.toSeq
        buffer(1) = buffer.getAs[Long](1) + (cols.toLong)
    }

    // This is how to merge two objects with the bufferSchema type.
    override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
        val dense_buffer1 = new breeze.linalg.DenseVector (buffer1.getSeq[Double](0).toArray)
        val dense_buffer2 = new breeze.linalg.DenseVector (buffer2.getSeq[Double](0).toArray)
        
        dense_buffer1 += dense_buffer2

        buffer1(0) = dense_buffer1.data.toSeq
        buffer1(1) = buffer1.getAs[Long](1) + buffer2.getAs[Long](1)
    }

    // This is where you output the final value, given the final value of your bufferSchema.
    override def evaluate(buffer: Row): Any = {
        val dense_buffer = new breeze.linalg.DenseVector (buffer.getSeq[Double](0).toArray)
        dense_buffer /= buffer.getAs[Long](1).toDouble

        dense_buffer.data.toSeq 
    }
}