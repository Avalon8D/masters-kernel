package kernel_lib

import linalg_impl.LinalgUtils

object KernelPack {
    def apply (
        X:breeze.linalg.DenseMatrix[Double],
        regularizer_diag:breeze.linalg.DenseVector[Double], 
        lambda:Double,
        kernel_func:(
            breeze.linalg.DenseVector[Double], 
            breeze.linalg.DenseVector[Double]
        ) => Double
    ) = {
        val kpack = new KernelPack (
            kernel_func
        )

        kpack.update (X, regularizer_diag, lambda)

        kpack
    }

    
    def knorm_factory (
        kernel_func:(
            breeze.linalg.DenseVector[Double], 
            breeze.linalg.DenseVector[Double]
        ) => Double
    ) = {
        org.apache.spark.sql.functions.udf (
            (y:Seq[Double]) => KernelUtils.kernel_norm (
                new breeze.linalg.DenseVector (y.toArray),
                kernel_func
            )
        )
    }
    
    def kproj_factory (
        X:breeze.linalg.DenseMatrix[Double],
        sqrt_GX:breeze.linalg.DenseMatrix[Double],
        kernel_func:(
            breeze.linalg.DenseVector[Double], 
            breeze.linalg.DenseVector[Double]
        ) => Double
    ) = {
        org.apache.spark.sql.functions.udf (
            (y:Seq[Double]) => KernelUtils.kernel_proj (
                X, new breeze.linalg.DenseVector (y.toArray), sqrt_GX, kernel_func
            ).data.toSeq
        )
    }
    
    def kleverage_factory (
        X:breeze.linalg.DenseMatrix[Double],
        sqrt_GX:breeze.linalg.DenseMatrix[Double],
        kernel_func:(
            breeze.linalg.DenseVector[Double], 
            breeze.linalg.DenseVector[Double]
        ) => Double
    ) = {
        org.apache.spark.sql.functions.udf (
            (y:Seq[Double]) => {
                val dense_y = new breeze.linalg.DenseVector (y.toArray)
                val y_proj = KernelUtils.kernel_proj (
                    X, dense_y, sqrt_GX, kernel_func
                )

                KernelUtils.kernel_norm (
                    dense_y, kernel_func
                ) - (y_proj.t * y_proj)
            }:Double
        )
    }
}

class KernelPack private (
    val kernel_func:(
        breeze.linalg.DenseVector[Double], 
        breeze.linalg.DenseVector[Double]
    ) => Double
) extends Serializable {
    private var _kproj_udf:org.apache.spark.sql.expressions.UserDefinedFunction = null
    private var _kleverage_udf:org.apache.spark.sql.expressions.UserDefinedFunction = null
    
    def kproj_udf = _kproj_udf
    def kleverage_udf = _kleverage_udf

    val knorm_udf = KernelPack.knorm_factory (kernel_func)

    def update (
        X:breeze.linalg.DenseMatrix[Double],
        regularizer_diag:breeze.linalg.DenseVector[Double], 
        lambda:Double
    ):Unit = {
        val sqrt_GX = KernelUtils.eval_chol (
            X, regularizer_diag, 
            lambda, this.kernel_func
        )
        
        this._kproj_udf = KernelPack.kproj_factory (X, sqrt_GX, this.kernel_func)
        this._kleverage_udf = KernelPack.kleverage_factory (X, sqrt_GX, this.kernel_func)
    }
}