package kernel_lib

import linalg_impl.LinalgUtils

// write ridge regressor udfs corresponding to predicts

object Ridge {
    // one version for each useful case
    // no work to be done, just incorporating the values and returning the fited class
    def fit (
        X:breeze.linalg.DenseMatrix[Double],
        sqrt_GX:breeze.linalg.DenseMatrix[Double],
        f_x:breeze.linalg.DenseVector[Double],
        kernel_func:(breeze.linalg.DenseVector[Double], breeze.linalg.DenseVector[Double]) => Double
    ) = {
        val alpha = new breeze.linalg.DenseVector[Double] (f_x.data.clone ())
        LinalgUtils.inplace_lower_triangular_solve (sqrt_GX, alpha, trans_opt="N")
        LinalgUtils.inplace_lower_triangular_solve (sqrt_GX, alpha, trans_opt="T")
        
        new Ridge_SO (X, alpha, kernel_func)
    }

    def fit (
        X:breeze.linalg.DenseMatrix[Double],
        sqrt_GX:breeze.linalg.DenseMatrix[Double],
        F_x:breeze.linalg.DenseMatrix[Double],
        kernel_func:(breeze.linalg.DenseVector[Double], breeze.linalg.DenseVector[Double]) => Double
    ) = {
        val Alpha = new breeze.linalg.DenseMatrix[Double] (F_x.rows, F_x.cols, F_x.data.clone ())
        LinalgUtils.inplace_lower_triangular_solve (sqrt_GX, Alpha, trans_opt="N")
        LinalgUtils.inplace_lower_triangular_solve (sqrt_GX, Alpha, trans_opt="T")
        
        new Ridge_MO (X, Alpha, kernel_func)
    }
    // version 
}

class Ridge_SO (
    val X:breeze.linalg.DenseMatrix[Double],
    val alpha:breeze.linalg.DenseVector[Double],
    val kernel_func:(breeze.linalg.DenseVector[Double], breeze.linalg.DenseVector[Double]) => Double
) {
    def predict (
        y:breeze.linalg.DenseVector[Double] // y value at which predict de value f(y)
    ) = {
        val KXy = KernelUtils.kernel_cross (X, y, kernel_func)
        alpha.t * (KXy)
    }

    def predict (
        Y:breeze.linalg.DenseMatrix[Double] // y values at which predict de values f(y)
    ) = {
        val KXY = KernelUtils.kernel_crosses (X, Y, kernel_func)
        alpha.t * (KXY)
    }
}

class Ridge_MO (
    val X:breeze.linalg.DenseMatrix[Double],
    val Alpha:breeze.linalg.DenseMatrix[Double],
    val kernel_func:(breeze.linalg.DenseVector[Double], breeze.linalg.DenseVector[Double]) => Double
) {
    def predict (
        y:breeze.linalg.DenseVector[Double] // y value at which predict de value f(y)
    ) = {
        val KXy = KernelUtils.kernel_cross (X, y, kernel_func)
        KXy.t * Alpha
    }

    def predict (
        Y:breeze.linalg.DenseMatrix[Double] // y values at which predict de values f(y)
    ) = {
        val KXY = KernelUtils.kernel_crosses (X, Y, kernel_func)
        KXY.t * Alpha
    }
}

// write ridge classifier, one vs all