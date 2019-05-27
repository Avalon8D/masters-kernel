package kernel_lib

object KernelUtils extends Serializable {    
    val kernel_norm = (
        x:breeze.linalg.DenseVector[Double], 
        kernel_func:(breeze.linalg.DenseVector[Double], breeze.linalg.DenseVector[Double]) => Double
    ) => kernel_func (x, x)

    def kernel_cross (
        X:breeze.linalg.DenseMatrix[Double],
        y:breeze.linalg.DenseVector[Double],
        kernel_func:(breeze.linalg.DenseVector[Double], breeze.linalg.DenseVector[Double]) => Double
    ):breeze.linalg.DenseVector[Double] = {
        X(::, breeze.linalg.*).map (
            (x:breeze.linalg.DenseVector[Double]) => kernel_func (x, y)
        ).t
    }

    def kernel_proj (
        X:breeze.linalg.DenseMatrix[Double],
        y:breeze.linalg.DenseVector[Double],
        sqrt_GX:breeze.linalg.DenseMatrix[Double],
        kernel_func:(breeze.linalg.DenseVector[Double], breeze.linalg.DenseVector[Double]) => Double
    ):breeze.linalg.DenseVector[Double] = {
        val y_proj = kernel_cross (X, y, kernel_func)
        
        LinalgUtils.inplace_lower_triangular_solve (sqrt_GX, y_proj, trans_opt="N")
        
        y_proj
    }
    
    // alters out vector's content
    def buff_kernel_proj (
        X:breeze.linalg.DenseMatrix[Double],
        y:breeze.linalg.DenseVector[Double],
        sqrt_GX:breeze.linalg.DenseMatrix[Double],
        kernel_func:(breeze.linalg.DenseVector[Double], breeze.linalg.DenseVector[Double]) => Double,
        out:breeze.linalg.DenseVector[Double]
    ):Unit = {
        for (i <- 0 until X.cols) {
            out (i) = kernel_func (X(::, i), y)
        }
        
        LinalgUtils.inplace_lower_triangular_solve (sqrt_GX, out, trans_opt="N")
    }

    def kernel_crosses (
        X:breeze.linalg.DenseMatrix[Double],
        Y:Iterator[breeze.linalg.DenseVector[Double]],
        kernel_func:(breeze.linalg.DenseVector[Double], breeze.linalg.DenseVector[Double]) => Double
    ):breeze.linalg.DenseMatrix[Double] = {
        // does Y then X because column major
        val kernel_cross_array = Y.flatMap (
            (y:breeze.linalg.DenseVector[Double]) => {
                X(::, breeze.linalg.*).iterator.map (
                    (x:breeze.linalg.DenseVector[Double]) => kernel_func (x, y)
                )
            }
        ).toArray

        new breeze.linalg.DenseMatrix (
            X.cols, kernel_cross_array.length / X.cols, 
            kernel_cross_array
        )
    }
    
    def kernel_projs (
        X:breeze.linalg.DenseMatrix[Double],
        Y:Iterator[breeze.linalg.DenseVector[Double]],
        sqrt_GX:breeze.linalg.DenseMatrix[Double],
        kernel_func:(breeze.linalg.DenseVector[Double], breeze.linalg.DenseVector[Double]) => Double
    ):breeze.linalg.DenseMatrix[Double] = {
        val Y_proj = kernel_crosses (X, Y, kernel_func)
        
        LinalgUtils.inplace_lower_triangular_solve (sqrt_GX, Y_proj, trans_opt="N")
        
        Y_proj
    }
    
    def kernel_leverages (
        X:breeze.linalg.DenseMatrix[Double],
        Y:Iterator[breeze.linalg.DenseVector[Double]],
        sqrt_GX:breeze.linalg.DenseMatrix[Double],
        kernel_func:(breeze.linalg.DenseVector[Double], breeze.linalg.DenseVector[Double]) => Double
    ):breeze.linalg.DenseVector[Double] = {
        val Y_Seq = Y.toSeq
        val Y_proj = kernel_projs (X, Y_Seq.iterator, sqrt_GX, kernel_func)
        
        Y_proj :*= Y_proj
        
        val residual = breeze.linalg.sum (Y_proj.t (breeze.linalg.*, ::))
        
        for ((i, aY_i) <- (0 until residual.length).zip (Y_Seq)) {
            residual (i) = kernel_norm (aY_i, kernel_func) - residual (i)
        }
        
        residual
    }

    def kernel_gram (
        X:breeze.linalg.DenseMatrix[Double],
        kernel_func:(breeze.linalg.DenseVector[Double], breeze.linalg.DenseVector[Double]) => Double
    ):breeze.linalg.DenseMatrix[Double] = {
        val gram = new breeze.linalg.DenseMatrix[Double](X.cols, X.cols)
        var k_ij = 0.0
        var Xi:breeze.linalg.DenseVector[Double] = null

        for (i <- 0 until X.cols) {
            Xi = X(::, i)

            for (j <- 0 until i) {
                k_ij = kernel_func (Xi, X(::, j))
                gram(i, j) = k_ij
                gram(j, i) = k_ij
            }

            gram (i, i) = kernel_func (Xi, Xi)
        }

        gram
    }
    
    def inplace_kernel_gram (
        X:breeze.linalg.DenseMatrix[Double],
        kernel_func:(breeze.linalg.DenseVector[Double], breeze.linalg.DenseVector[Double]) => Double,
        out:breeze.linalg.DenseMatrix[Double]
    ):Unit = {
        assert (
            X.cols == out.cols && X.cols == out.rows, 
            """Output Matrix did not match input X dimension
              |Output dims ${out.rows} x ${out.cols}
              |X dim ${X.cols}"""
        )
        
        var k_ij = 0.0
        var Xi:breeze.linalg.DenseVector[Double] = null

        for (i <- 0 until X.cols) {
            Xi = X(::, i)

            for (j <- 0 until i) {
                k_ij = kernel_func (Xi, X(::, j))
                out(i, j) = k_ij
                out(j, i) = k_ij
            }

            out (i, i) = kernel_func (Xi, Xi)
        }
    }
    
    def eval_chol (
        X:breeze.linalg.DenseMatrix[Double], 
        regularizer_diag:breeze.linalg.DenseVector[Double], 
        lambda:Double,
        kernel_func:(breeze.linalg.DenseVector[Double], breeze.linalg.DenseVector[Double]) => Double
    ):breeze.linalg.DenseMatrix[Double] = {
        val eventual_out_mat = kernel_gram (X, kernel_func)

        // slightly inefficient but garantees a pure function
        eventual_out_mat += breeze.linalg.diag (lambda * regularizer_diag)
        
        //cholesky
        LinalgUtils.implace_cholesky (eventual_out_mat)
        
        eventual_out_mat
    }
    
    def implace_eval_chol (
        X:breeze.linalg.DenseMatrix[Double], 
        regularizer_diag:breeze.linalg.DenseVector[Double], 
        lambda:Double,
        kernel_func:(breeze.linalg.DenseVector[Double], breeze.linalg.DenseVector[Double]) => Double,
        eventual_out_mat:breeze.linalg.DenseMatrix[Double]
    ):Unit = {
        inplace_kernel_gram (X, kernel_func, eventual_out_mat)

        // slightly inefficient but garantees a pure function
        eventual_out_mat += breeze.linalg.diag (lambda * regularizer_diag)
        
        //cholesky
        LinalgUtils.implace_cholesky (eventual_out_mat)  
    }
}