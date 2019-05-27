package kernel_lib
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
}