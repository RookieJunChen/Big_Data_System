import scala.collection.mutable.ArrayBuffer
import scala.io._

// 用于执行矩阵运算的矩阵类
class Matrix(private val data:Array[Double], val rownum:Int){
    val colnum = (data.length.toDouble/rownum).ceil.toInt
    val matrix:Array[Array[Double]]={ 
        val matrix:Array[Array[Double]] = Array.ofDim[Double](rownum,colnum)
        for(i <- 0 until rownum){
            for(j <- 0 until colnum){
                val index = i * colnum + j
                matrix(i)(j) = if(data.isDefinedAt(index)) data(index) else 0
            }
        }
        matrix
    }

    override def toString = {
        var str = ""
        matrix.map((p:Array[Double]) => {p.mkString(" ")}).mkString("\n")
    }

    def mat(row:Int,col:Int) = {
        matrix(row - 1)(col - 1)
    }

    // 矩阵与矩阵乘法
    def *(a : Matrix) : Matrix = {
        if(this.colnum != a.rownum){
            println("Wrong!")
            var ans = new Matrix(this.data,this.rownum)
            ans.asInstanceOf[Matrix]
        }else{
            val data:ArrayBuffer[Double] = ArrayBuffer()
            for(i <- 0 until this.rownum){
                for(j <- 0 until a.colnum){
                    var num = 0.0
                    for(k <- 0 until this.colnum){
                        num += this.matrix(i)(k) * a.matrix(k)(j)
                    }
                data += num
                }
            }
            var ans = new Matrix(data.toArray,this.rownum)
            ans.asInstanceOf[Matrix]
        }
    }

    // 矩阵乘常数
    def *(a:Double) : Matrix = {
        val data:ArrayBuffer[Double] = ArrayBuffer()
        for(i <- 0 until this.rownum){
            for(j <- 0 until this.colnum){
                data += this.matrix(i)(j) * a
            }
        }
        var ans = new Matrix(data.toArray,this.rownum)
        ans.asInstanceOf[Matrix]
    }

    // 矩阵间加法
    def +(a : Matrix) : Matrix = {
        if(this.rownum != a.rownum || this.colnum != a.colnum){
            println("Wrong!")
            var ans = new Matrix(this.data,this.rownum)
            ans.asInstanceOf[Matrix]
        }else{
            val data:ArrayBuffer[Double] = ArrayBuffer()
            for(i <- 0 until this.rownum){
                for(j <- 0 until this.colnum){
                    data += this.matrix(i)(j) + a.matrix(i)(j)
                }
            }
            var ans = new Matrix(data.toArray,this.rownum)
            ans.asInstanceOf[Matrix]
        }       
    }

    // 矩阵间减法
    def -(a : Matrix) : Matrix = {
        if(this.rownum != a.rownum || this.colnum != a.colnum){
            println("Wrong!")
            var ans = new Matrix(this.data,this.rownum)
            ans.asInstanceOf[Matrix]
        }else{
            val data:ArrayBuffer[Double] = ArrayBuffer()
            for(i <- 0 until this.rownum){
                for(j <- 0 until this.colnum){
                    data += this.matrix(i)(j) - a.matrix(i)(j)
                }
            }
            var ans = new Matrix(data.toArray,this.rownum)
            ans.asInstanceOf[Matrix]
        }       
    }


    // 矩阵转置
    def transpose() : Matrix = {
        val transposeMatrix = for (i <- Array.range(0,colnum)) yield {
             for (rowArray <- this.matrix) yield rowArray(i)
            }
        var ans = new Matrix(transposeMatrix.flatten,colnum)
        ans.asInstanceOf[Matrix]
    }

}

object Linear_Regressuion{
    def main(args:Int) : Unit = {
        var alpha = 0.001   // 学习率alpha
        var x = ArrayBuffer[Double]()
        var y = ArrayBuffer[Double]()
        val data = Source.fromFile("/home/dsjxtjc/2021214308/linear_regression/linear_data.txt")
        var cnt =  0    //   矩阵行数计数器
        for(line <- data.getLines)
        {
            cnt += 1
            var parts = line.split(",")
            // 逗号前一部分为y
            y.append(parts(0).toDouble)
            // 逗号后半部分为多元x，并以空格分开
            var x_part : Array[Double] = parts(1).split(" ").map(_.toDouble)
            x.append(1.0)
            for(i <- 0 until x_part.size)
                x.append(x_part(i))
        }
        data.close
        // 使用矩阵接口构造X、Y的矩阵
        var X_matrix = new Matrix(x.toArray, cnt)
        var Y_matrix = new Matrix(y.toArray, cnt)

        // 初始化权重矩阵(初始化为1)
        var w_array = ArrayBuffer[Double]()
        for(k <- 0 until X_matrix.colnum)
            w_array.append(1)
        var w = new Matrix(w_array.toArray, X_matrix.colnum)

        for(i <- 0 until 150){
            // 对用向量表示的n元线性回归梯度下降法进行迭代
            var J : Matrix = ( ((X_matrix*w) - Y_matrix).transpose) *  ((X_matrix*w) - Y_matrix) * (1.0/(2 * X_matrix.rownum))
            println("iteration " + i + ":  " + J)
            // 更新权重矩阵
            w = w - (X_matrix.transpose) * ((X_matrix * w) - Y_matrix) * (alpha) * 2
        }

        println("w:")
        println(w)
    }
}

