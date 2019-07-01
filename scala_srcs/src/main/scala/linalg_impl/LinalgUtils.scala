package linalg_impl

import com.github.fommil.netlib.LAPACK.{getInstance => lapack}

object LinalgUtils extends Serializable {
    val lapack_trans_opts = Set ("T", "N", "C")

    def inplace_lower_triangular_solve (
        L:breeze.linalg.DenseMatrix[Double], 
        x: breeze.linalg.DenseVector[Double],
        trans_opt:String
    ):Unit = {
        assert (lapack_trans_opts.contains (trans_opt))
        val N = L.rows
        val info = new  org.netlib.util.intW (0)
        lapack.dtrtrs(
            "L" /* lower triangular */,
            "N",
            "N",
            N /* number of rows */,
            1 /* number of right hand sides */,
            L.data,
            scala.math.max(1, N) /* LDA */,
            x.data,
            scala.math.max(1, N) /* LDB */,
            info
        )
        
        assert(info.`val` >= 0)

        if (info.`val` > 0)
        throw new breeze.linalg.MatrixSingularException
    }
    
    def inplace_lower_triangular_solve (
        L:breeze.linalg.DenseMatrix[Double], 
        X: breeze.linalg.DenseMatrix[Double],
        trans_opt:String
    ):Unit = {
        assert (lapack_trans_opts.contains (trans_opt))
        val N = L.rows
        val info = new  org.netlib.util.intW (0)
        lapack.dtrtrs(
            "L" /* lower triangular */,
            "N",
            "N",
            N /* number of rows */,
            X.cols /* number of right hand sides */,
            L.data,
            scala.math.max(1, N) /* LDA */,
            X.data,
            scala.math.max(1, N) /* LDB */,
            info
        )
        
        assert(info.`val` >= 0)

        if (info.`val` > 0)
        throw new breeze.linalg.NotConvergedException(breeze.linalg.NotConvergedException.Iterations)
    }
    
    def implace_cholesky (X: breeze.linalg.DenseMatrix[Double]): Unit = {
        // set upper triangle to 0
        for (i <- 0 until X.rows; j <- i + 1 until X.cols) {
            X (i, j) = 0.0
        }

        val N = X.rows
        val info = new  org.netlib.util.intW (0)
        lapack.dpotrf(
            "L" /* lower triangular */,
            N /* number of rows */,
            X.data,
            scala.math.max(1, N) /* LDA */,
            info
        )
        // A value of info.`val` < 0 would tell us that the i-th argument
        // of the call to dpotrf was erroneous (where i == |info.`val`|).
        assert(info.`val` >= 0)

        if (info.`val` > 0)
        throw new breeze.linalg.NotConvergedException(breeze.linalg.NotConvergedException.Iterations)
    }
    
    val norm_sqr_udf = org.apache.spark.sql.functions.udf (
        (y:Seq[Double]) => {
            val dense_y = new breeze.linalg.DenseVector (y.toArray)

            dense_y.t * dense_y
        }
    )
    
    val norm_udf = org.apache.spark.sql.functions.udf (
        (y:Seq[Double]) => {
            val dense_y = new breeze.linalg.DenseVector (y.toArray)

            breeze.linalg.norm (dense_y)
        }
    )
    
    val normalize_udf = org.apache.spark.sql.functions.udf (
        (y:Seq[Double]) => {
            val dense_y = new breeze.linalg.DenseVector (y.toArray)

            dense_y /= breeze.linalg.norm (dense_y)
            
            dense_y.data.toSeq
        }
    )

    val cap_to_one_udf = org.apache.spark.sql.functions.udf (
        scala.math.min (_:Double, 1.0)
    )
    
    // maybe unecessarely overhady, but o well
    def split_arrays[T] (cols:Int, arr:Seq[T]):Seq[Seq[T]] = {
        val rows = arr.length / cols

        (0 until arr.length by rows).map (
            (i:Int) => arr.slice (i, i + rows).to[Seq]
        ).to[Seq]
    }

    // sections contiguous array data based on length value
    val split_arrays_udf = org.apache.spark.sql.functions.udf (
        split_arrays[Double] _
    )

    def add_vec_to_vecs (vec_to_add:Seq[Double]) = {
        val dense_vec_to_add = new breeze.linalg.DenseVector (vec_to_add.toArray)

        org.apache.spark.sql.functions.udf (
            (cols:Int, Y:Seq[Double]) => {
                val Y_dense = new breeze.linalg.DenseMatrix (
                    Y.length / cols, cols,
                    Y.toArray
                )

                // add to every line
                val res = Y_dense (::, breeze.linalg.*) + dense_vec_to_add
                (res.cols.toLong, res.data.toSeq)
            }:(Long, Seq[Double])
        )
    }

    def mul_vecs_by_vec (vec_to_mul:Seq[Double]) = {
        val dense_vec_to_mul = new breeze.linalg.DenseVector (vec_to_mul.toArray)

        org.apache.spark.sql.functions.udf (
            (cols:Int, Y:Seq[Double]) => {
                val Y_dense = new breeze.linalg.DenseMatrix (
                    Y.length / cols, cols,
                    Y.toArray
                )

                // multiply every line
                val res = Y_dense (::, breeze.linalg.*) * dense_vec_to_mul
                (res.cols.toLong, res.data.toSeq)
            }:(Long, Seq[Double])
        )
    }

    def div_vecs_by_vec (vec_to_div:Seq[Double]) = {
        val dense_vec_to_div = 1.0 / new breeze.linalg.DenseVector (vec_to_div.toArray)

        org.apache.spark.sql.functions.udf (
            (cols:Int, Y:Seq[Double]) => {
                val Y_dense = new breeze.linalg.DenseMatrix (
                    Y.length / cols, cols,
                    Y.toArray
                )

                // multiply every line
                val res = Y_dense (::, breeze.linalg.*) * dense_vec_to_div
                (res.cols.toLong, res.data.toSeq)
            }:(Long, Seq[Double])
        )
    }

    def add_scalar_to_vecs (scalar_to_add:Double) = {
        org.apache.spark.sql.functions.udf (
            (cols:Int, Y:Seq[Double]) => {
                val Y_dense = new breeze.linalg.DenseMatrix (
                    Y.length / cols, cols,
                    Y.toArray
                )

                Y_dense += scalar_to_add
                (Y_dense.cols.toLong, Y_dense.data.toSeq)
            }:(Long, Seq[Double])
        )
    }

    def mul_vecs_by_scalar (scalar_to_mul:Double) = {
        org.apache.spark.sql.functions.udf (
            (cols:Int, Y:Seq[Double]) => {
                val Y_dense = new breeze.linalg.DenseMatrix (
                    Y.length / cols, cols,
                    Y.toArray
                )

                Y_dense *= scalar_to_mul
                (Y_dense.cols.toLong, Y_dense.data.toSeq)
            }:(Long, Seq[Double])
        )
    }

    def div_vecs_by_scalar (scalar_to_div:Double) = {
        val iscalar_to_div = 1 / scalar_to_div

        org.apache.spark.sql.functions.udf (
            (cols:Int, Y:Seq[Double]) => {
                val Y_dense = new breeze.linalg.DenseMatrix (
                    Y.length / cols, cols,
                    Y.toArray
                )

                Y_dense *= iscalar_to_div
                (Y_dense.cols.toLong, Y_dense.data.toSeq)
            }:(Long, Seq[Double])
        )
    }

    def square_vecs = org.apache.spark.sql.functions.udf (
        (cols:Int, Y:Seq[Double]) => {
            val Y_dense = new breeze.linalg.DenseVector (
                Y.toArray
            )

            Y_dense *= Y_dense
            (cols.toLong, Y_dense.data.toSeq)
        }:(Long, Seq[Double])
    )

    def reciprocal_vecs = org.apache.spark.sql.functions.udf (
        (cols:Int, Y:Seq[Double]) => {
            val Y_dense = new breeze.linalg.DenseMatrix (
                Y.length / cols, cols,
                Y.toArray
            )

            val res = 1.0 / Y_dense
            (res.cols.toLong, res.data.toSeq)
        }:(Long, Seq[Double])
    )
}