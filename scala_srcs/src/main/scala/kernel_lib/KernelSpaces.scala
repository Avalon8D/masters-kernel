package kernel_lib

import linalg_impl.LinalgUtils

object KernelSpaces {
    def apply (
        X:breeze.linalg.DenseMatrix[Double],
        regularizer_diag:breeze.linalg.DenseVector[Double], 
        X_ids:Seq[Long],
        lambda:Double,
        kernel_func:(
            breeze.linalg.DenseVector[Double], 
            breeze.linalg.DenseVector[Double]
        ) => Double,
        Xs_max_count:Int
    ) = {
        val kspaces = new KernelSpaces (
            new Array[breeze.linalg.DenseMatrix[Double]] (Xs_max_count), // Xs
            new Array[breeze.linalg.DenseVector[Double]] (Xs_max_count), // regularizer_diags
            new Array[breeze.linalg.DenseMatrix[Double]] (Xs_max_count), // sqrt_GXs
            new Array[Seq[Long]] (Xs_max_count), // Xs_ids
            kernel_func
        )

        kspaces.update_lambda (lambda)

        kspaces.append (
            X,
            regularizer_diag,
            X_ids
        )
        
        kspaces
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

    def knorms_factory (
        kernel_func:(
            breeze.linalg.DenseVector[Double], 
            breeze.linalg.DenseVector[Double]
        ) => Double
    ) = {
        org.apache.spark.sql.functions.udf (
            (cols:Int, Y:Seq[Double]) => {
                val Y_dense = new breeze.linalg.DenseMatrix (
                    Y.length / cols, cols,
                    Y.toArray
                )

                (0 until cols).map {
                    case i => KernelUtils.kernel_norm (
                        Y_dense (::, i),
                        kernel_func
                    )
                }.toSeq
            }:Seq[Double]
        )
    }
    
    def kproj_factory (
        Xs:Seq[breeze.linalg.DenseMatrix[Double]],
        sqrt_GXs:Seq[breeze.linalg.DenseMatrix[Double]],
        kernel_func:(
            breeze.linalg.DenseVector[Double], 
            breeze.linalg.DenseVector[Double]
        ) => Double
    ) = {
        val proj_len = Xs.map (_.cols).sum
        val kspace_pairs = Xs.zip (sqrt_GXs)

        org.apache.spark.sql.functions.udf (
            (y:Seq[Double]) => {
                val dense_y = new breeze.linalg.DenseVector (y.toArray)
                val y_proj = new breeze.linalg.DenseVector[Double] (proj_len) 

                var offset:Int = 0

                // aparently pattern matching requires lowercase...
                for ((aX, sqrt_GX) <- kspace_pairs) {
                    KernelUtils.buff_kernel_proj (
                        aX, dense_y, sqrt_GX, kernel_func,
                        y_proj(offset until offset + aX.cols)
                    )
                }

                y_proj.data.toSeq
            }
        )
    }
    
    // leverage is evaluated via 
    // knorm - sum (leverages w.r.t. each X)
    // may turn out negative
    // there may be alternatives
    def pseudo_kleverage_factory (
        Xs:Seq[breeze.linalg.DenseMatrix[Double]],
        sqrt_GXs:Seq[breeze.linalg.DenseMatrix[Double]],
        kernel_func:(
            breeze.linalg.DenseVector[Double], 
            breeze.linalg.DenseVector[Double]
        ) => Double
    ) = {
        val buffer_len = Xs.map (_.cols).max
        val kspace_pairs = Xs.zip (sqrt_GXs)

        org.apache.spark.sql.functions.udf (
            (y:Seq[Double]) => {
                val dense_y = new breeze.linalg.DenseVector (y.toArray)
                val buff_y_proj = new breeze.linalg.DenseVector[Double] (buffer_len)

                var residual = KernelUtils.kernel_norm (
                    dense_y, kernel_func
                )

                // aparently pattern matching requires lowercase...
                for ((aX, sqrt_GX) <- kspace_pairs) {
                    KernelUtils.buff_kernel_proj (
                        aX, dense_y, sqrt_GX, kernel_func,
                        buff_y_proj(0 until aX.cols)
                    )
                    residual -= buff_y_proj(0 until aX.cols).t * buff_y_proj(0 until aX.cols)
                }

                residual
            }:Double
        )
    }

    def min_kleverage_factory (
        Xs:Seq[breeze.linalg.DenseMatrix[Double]],
        sqrt_GXs:Seq[breeze.linalg.DenseMatrix[Double]],
        kernel_func:(
            breeze.linalg.DenseVector[Double], 
            breeze.linalg.DenseVector[Double]
        ) => Double
    ) = {
        val buffer_len = Xs.map (_.cols).max
        val kspace_pairs = Xs.zip (sqrt_GXs)

        org.apache.spark.sql.functions.udf (
            (y:Seq[Double]) => {
                val dense_y = new breeze.linalg.DenseVector (y.toArray)
                val buff_y_proj = new breeze.linalg.DenseVector[Double] (buffer_len)

                val norm = KernelUtils.kernel_norm (
                    dense_y, kernel_func
                )

                // aparently pattern matching requires lowercase...
                kspace_pairs.map {
                    case (aX, sqrt_GX) => {
                        KernelUtils.buff_kernel_proj (
                            aX, dense_y, sqrt_GX, kernel_func,
                            buff_y_proj(0 until aX.cols)
                        )
                        norm - buff_y_proj(0 until aX.cols).t * buff_y_proj(0 until aX.cols)
                    }
                }.min
            }:Double
        )
    }

    def min_kleverages_factory (
        Xs:Seq[breeze.linalg.DenseMatrix[Double]],
        sqrt_GXs:Seq[breeze.linalg.DenseMatrix[Double]],
        kernel_func:(
            breeze.linalg.DenseVector[Double], 
            breeze.linalg.DenseVector[Double]
        ) => Double
    ) = {
        val buffer_len = Xs.map (_.cols).max
        val kspace_pairs = Xs.zip (sqrt_GXs)

        org.apache.spark.sql.functions.udf (
            (cols:Int, Y:Seq[Double]) => {
                val Y_dense = new breeze.linalg.DenseMatrix (
                    Y.length / cols, cols,
                    Y.toArray
                )

                val pairs_leverages = new breeze.linalg.DenseMatrix[Double](
                    Y_dense.cols, kspace_pairs.length
                )

                kspace_pairs.iterator.zip (
                    pairs_leverages (::, breeze.linalg.*).iterator
                ).foreach {
                    case ((aX, sqrt_GX), leverages) => {
                        KernelUtils.buff_kernel_leverages (
                            aX, Y_dense, sqrt_GX, kernel_func,
                            leverages
                        )
                    }
                }

                breeze.linalg.min (pairs_leverages (breeze.linalg.*, ::)).data.toSeq
            }:Seq[Double]
        )
    }
}

class KernelSpaces private (
    private val _Xs:Array[breeze.linalg.DenseMatrix[Double]],
    private val _regularizer_diags:Array[breeze.linalg.DenseVector[Double]],
    private val _sqrt_GXs:Array[breeze.linalg.DenseMatrix[Double]],
    private val _Xs_ids:Array[Seq[Long]],
    val kernel_func:(
        breeze.linalg.DenseVector[Double], 
        breeze.linalg.DenseVector[Double]
    ) => Double
) extends Serializable {
    private var _kproj_udf:org.apache.spark.sql.expressions.UserDefinedFunction = null
    private var _pseudo_kleverage_udf:org.apache.spark.sql.expressions.UserDefinedFunction = null
    private var _min_kleverage_udf:org.apache.spark.sql.expressions.UserDefinedFunction = null
    private var _min_kleverages_udf:org.apache.spark.sql.expressions.UserDefinedFunction = null
    
    private var _Xs_count:Int = 0
    private var _Xs_last_id:Int = 0
    
    // can be updated, updating all subspaces
    private var _lambda:Double = 0

    val Xs_length = this._Xs.length

    def kproj_udf = this._kproj_udf
    def pseudo_kleverage_udf = this._pseudo_kleverage_udf
    def min_kleverage_udf = this._min_kleverage_udf
    def min_kleverages_udf = this._min_kleverages_udf

    def lambda = this._lambda
    def Xs_count = this._Xs_count
    def Xs_last_id = this._Xs_last_id

    def Xs = this._Xs.slice (0, this._Xs_count).toSeq
    def sqrt_GXs = this._sqrt_GXs.slice (0, this._Xs_count).toSeq
    def regularizer_diags = this._regularizer_diags.slice (0, this._Xs_count).toSeq
    def Xs_ids = this._Xs_ids.slice (0, this._Xs_count).toSeq

    private def update_udfs:Unit = {
        // reconstructs udfs
        this._kproj_udf = KernelSpaces.kproj_factory (
            this._Xs.slice (0, this._Xs_count).toSeq, 
            this._sqrt_GXs.slice (0, this._Xs_count).toSeq, 
            this.kernel_func
        )
        
        this._pseudo_kleverage_udf = KernelSpaces.pseudo_kleverage_factory (
            this._Xs.slice (0, this._Xs_count).toSeq, 
            this._sqrt_GXs.slice (0, this._Xs_count).toSeq, 
            this.kernel_func
        )

        this._min_kleverage_udf = KernelSpaces.min_kleverage_factory (
            this._Xs.slice (0, this._Xs_count).toSeq, 
            this._sqrt_GXs.slice (0, this._Xs_count).toSeq, 
            this.kernel_func
        )

        this._min_kleverages_udf = KernelSpaces.min_kleverages_factory (
            this._Xs.slice (0, this._Xs_count).toSeq, 
            this._sqrt_GXs.slice (0, this._Xs_count).toSeq, 
            this.kernel_func
        )
    }

    def append (
        X:breeze.linalg.DenseMatrix[Double],
        regularizer_diag:breeze.linalg.DenseVector[Double], 
        X_ids:Seq[Long]
    ):Unit = {
        assert (this.Xs_length > this._Xs_count, "Cannot append anymore matrices, Array full")

        // adding new matrices and vectors
        this._Xs (this._Xs_count) = X
        this._regularizer_diags (this._Xs_count) = regularizer_diag
        this._Xs_ids (this._Xs_count) = X_ids
        
        this._sqrt_GXs (this._Xs_count) = KernelUtils.eval_chol (
            X, regularizer_diag, 
            this._lambda, this.kernel_func
        )

        // updates count
        this._Xs_count += 1
        this._Xs_last_id += 1

        this.update_udfs
    }

    // overwrites matrices when comming back around the ids
    def append_circular (
        X:breeze.linalg.DenseMatrix[Double],
        regularizer_diag:breeze.linalg.DenseVector[Double], 
        X_ids:Seq[Long]        
    ):Unit = {
        // caps out last id if necessary
        // helps with not needing to do it anywhere else
        if (this._Xs_last_id >= this.Xs_length) {
            this._Xs_last_id = 0
        }
        
        // adding new matrices and vectors
        this._Xs (this._Xs_last_id) = X
        this._regularizer_diags (this._Xs_last_id) = regularizer_diag
        this._Xs_ids (this._Xs_count) = X_ids
        
        this._sqrt_GXs (this._Xs_last_id) = KernelUtils.eval_chol (
            X, regularizer_diag, 
            this._lambda, this.kernel_func
        )

        // updates count and last id
        this._Xs_last_id += 1
        
        if (this._Xs_count < this.Xs_length) {
            this._Xs_count += 1
        }

        this.update_udfs
    }
    
    def remove_id (i:Int) {
        assert (i < this._Xs_count && i >= 0, f"Index ${i} out of interval [0, ${this._Xs_count})")
        
        // sets entries to null, because gc (?)
        this._Xs (i) = null
        this._sqrt_GXs (i) = null
        this._regularizer_diags (i) = null
        this._Xs_ids (i) = null
        
        // decrements length
        this._Xs_count -= 1
        
        // sets i to the highest index's (this._Xs_count) values
        this._Xs (i) = this._Xs (this._Xs_count)
        this._sqrt_GXs (i) = this._sqrt_GXs (this._Xs_count)
        this._regularizer_diags (i) = this._regularizer_diags (this._Xs_count)
        this._Xs_ids (i) = this._Xs_ids (this._Xs_count)
        
        this.update_udfs
    }

    // updates all subspaces to conform to new regularization
    // up to understanding how to update without reevaluating cholesky factors
    def update_lambda (
        lambda:Double
    ):Unit = {
        this._lambda = lambda

        // less memory leaky
        for (
            ((aX, sqrt_GX), regularizer_diag) <- this._Xs.slice (
                0, this._Xs_count
            ).zip (
                this._sqrt_GXs
            ).zip (this._regularizer_diags)
        ) {
            KernelUtils.implace_eval_chol (
                aX, regularizer_diag, 
                this._lambda, this.kernel_func,
                sqrt_GX
            )
        }
    }
}