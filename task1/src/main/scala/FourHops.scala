//------------------------------------------------------------------------------
// Fedor Sergeev
//------------------------------------------------------------------------------

import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd._
import scala.tools.nsc.io.File

object FourHops {
//------------------------------------------------------------------------------
    def main(args: Array[String]) = {
        Logger.getLogger("org").setLevel(Level.ERROR)
        Logger.getLogger("akka").setLevel(Level.ERROR)

        val spark = SparkSession.builder()
           .master("local[1]")
           .getOrCreate()
        
        spark.sparkContext.setLogLevel("ERROR")
        val datafile: String = "src/test/resources/cycledirected.csv"
        val graph = loadData(spark, datafile)
        
        // Count total vertices in the graph
        val totalVertices = countVertices(graph)
        println("Total Vertices", totalVertices)

        // Find two hop neighbors
        val two_hop = matrixMultiply(graph, totalVertices)
        //two_hop.collect().foreach(println)
        
        // Find four hop neighbors
        val quads = matrixMultiply(two_hop, totalVertices)
        
        quads.collect().foreach(println)
        
        spark.stop()
    }
//------------------------------------------------------------------------------

    type GRAPH = RDD[(Int, Int)]
    
//------------------------------------------------------------------------------
    /*
    This function counts the number of distinct nodes in the input graph. Since each node 
    has atleast degree 1, it should appear in some pair (v1, v2).
    */
    def countVertices(graph: GRAPH): Int = {
        val n_vert = graph
            // convert set of edges to set of all vertices with replicates
            // e.g. {(i, j)} -> {i, j, ...}
            .flatMap(edge => Seq[Int](edge._1, edge._2))
            // remove replicates
            .distinct()
            // calculate the number of vertices and explicitly convert to int
            .count().toInt

        return n_vert
    }
//------------------------------------------------------------------------------
//  MapReduce
//------------------------------------------------------------------------------
    /*
    This function should output tuples of the form
    ((i, j), (identifying_matrix, identifying_index))
    
    (i, j) refers to the position in output matrix.
    
    Identifying matrix refers to A and B in the matrix product A.B, use 1 as identifier 
    for A and 0 for B.
    
    Identifying index helps multiply correct elements of A and B.
    */
    def mmMapper(N: Int, index: (Int, Int)): List[((Int, Int), (Int, Int))] = {
        // identifiers for matrices A and B
        val A_id = 1
        val B_id = 0

        // unpack edge into vertices i and j
        val i = index._1
        val j = index._2

        // create an iterate for all matrix positions (0 to total number of vertices - 1)
        val k_list = (0 to N - 1).toList

        // create maps for multiplication
        // computation of P[i,k] requires A[i,j]
        // computation of P[k,j] requires B[i,j]
        val listA = k_list.map(k => ((i,k), (A_id, j)))
        val listB = k_list.map(k => ((k,j), (B_id, i)))

        // concatenate lists (to be passed to the reducer)
        return listA ++ listB
    }

//------------------------------------------------------------------------------

    /*
    The reducer reduces row i of Matrix A and column j of matrix B. Note that it only receives 
    non zero elements on the row/column. The identifying matrix and identifying index in the 
    specified row/column is present in the Iterable[(Int, Int)].

    It must now produce the matrix product value (i, j, value). Set the value to 1 if it is 
    greater than 0 else let it be 0. Refer to test3 and test4 for an example case.
    */
    def mmReducer(productElements: ((Int, Int), Iterable[(Int, Int)])): (Int, Int, Int) = {
        // identifiers for matrices A and B
        val A_id = 1
        val B_id = 0

        // unpack productElements into the vertices i and j
        val i = productElements._1._1
        val j = productElements._1._2

        val matr_els = productElements._2
            // group by matrix identifier (1 for A, 0 for B)
            .groupBy(_._1)
            // here map structure is {matrix id -> map{matrix id -> vertice}}
            // we convert it to {matrix id -> list of vertices}
            .map { case (k, v) => (k, v.map(_._2))}

        // extract non-zero elements for A and B
        val A_vec = matr_els(A_id).toSet
        val B_vec = matr_els(B_id).toSet

        // intersection is the non-zero elements of P = A.B
        val inter = A_vec.intersect(B_vec)
        // todo: set intersection is slow => this procedure can be optimized?

        // return edge and non-edge (i,j) (non-edges are filtered out in the multiplication)
        return (i, j, if (inter.isEmpty) 0 else 1)
    }
//------------------------------------------------------------------------------    
    
    /*
    This function multiplies the adjacency matrix to itself: A.B = A.A = A^2 
    
    The input consists of only pairs which correspond to a directed edge. Hence, if there is no 
    edge between v1 and v2, the pair (v1, v2) will not appear in the input.
    
    Similar to the input, the output must consist only of pairs which correspond to a positive 
    value after multiplication. Hence if, A^2[v1, v2] = 0, the pair (v1, v2) should not appear 
    in the output. 

    Note that this is sufficient for our goal of identifying all 4-hop neighbors of a node. 
    */
    def matrixMultiply(matrix: GRAPH, N: Int): GRAPH = {
        val mult_matrix = matrix
            // perform map to obtain ((i, j), (identifying_matrix, identifying_index))
            .flatMap(edge => mmMapper(N, edge))
            // group by edges
            .groupByKey()
            // perform reduce to obtain {(i, j, 1 if edge is present or 0 otherwise)}
            .map(productElements => mmReducer(productElements))
            // get rid of zero elements of the product matrix aka (i,j,0)
            .filter(matrix_el => matrix_el._3 > 0)
            // since 0s are filtered, drop the "1" referring to non-zero elements: (i,j,1) -> (i,j) 
            .map(matrix_el => (matrix_el._1, matrix_el._2))

        return mult_matrix
    }

//------------------------------------------------------------------------------
//  Auxiliary functions
//------------------------------------------------------------------------------

    def loadData(spark: SparkSession, datafile: String): GRAPH = {
        import spark.implicits._
        
        val graph = spark.read.options(Map("header"->"false"))
            .csv(datafile)
            .rdd
            .map(r => (r.getString(0).toInt, r.getString(1).toInt))
        
        graph
    }

//------------------------------------------------------------------------------
}
