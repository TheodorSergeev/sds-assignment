// Fedor Sergeev
package task2

import scala.io.Source

object Task2 {
  /*
   * Main Class. This will be used to test your code.
   * These are just example queries and tests. The final tests are private.
   */

   /*
    * Do not modify the following messageType strings.
    * You can however add any more you like.
    */
    val STORE = "STORE"
    val STORE_SUCCESS = "STORE_SUCCESS"
    val STORE_FAILURE = "STORE_FAILURE" // You should never have to return this.
    val RETRIEVE = "RETRIEVE"
    val GET_NEIGHBOURS = "GET_NEIGHBOURS"
    val NEIGHBOURS_RESPONSE = "NEIGHBOURS_RESPONSE"
    val RETRIEVE_SUCCESS = "RETRIEVE_SUCCESS"
    val RETRIEVE_FAILURE = "RETRIEVE_FAILURE"
    val INTERNAL_ERROR = "INTERNAL_ERROR"
    val USER = "USER"
    val nodes_may_fail = 4

  def main(args: Array[String]) = {
    val router: Router = new Router() // Instantiate the router

    // Read the overlay adjacency list
    val source = Source.fromFile("overlay.txt")
    val routerInfo: scala.collection.mutable.Map[String, MyNode] = scala.collection.mutable.Map()
    for (line <- source.getLines()) {
      val adj = line.split(" ")
      val myNode = new MyNode(adj(0), adj(1).toInt, adj.drop(2).toVector, router)
      routerInfo += (adj(0) -> myNode)
    }
    source.close()

    router.addNodes(routerInfo.toMap) // Add new nodes to the router

    /* QUERIES */

    var m = router.sendMessage(USER, "u2", new Message(USER, STORE, "key1->value1")) // Store key
    println(m.messageType + " " + m.data)

    m = router.sendMessage(USER, "u2", new Message(USER, RETRIEVE, "key1")) // Retrieve key
    println(m.messageType + " " + m.data)

    m = router.sendMessage(USER, "u3", new Message(USER, RETRIEVE, "key1")) // Retrieve from another node
    println(m.messageType + " " + m.data)

    /* Check the correctness of the store. The stored keys should be present in at least one node */
    println(routerInfo("u1").returnStore)
    println(routerInfo("u2").returnStore)
    println(routerInfo("u3").returnStore)
    println(routerInfo("u4").returnStore)
    println(routerInfo("u5").returnStore)
    println(routerInfo("u6").returnStore)
    println(routerInfo("u7").returnStore)
    println(routerInfo("u8").returnStore)

    /*
    /* Crash some nodes*/
    var failing_node = routerInfo("u1")
    var all_keys = failing_node.returnStore.keySet
    all_keys.foreach {
      failing_node.removeKey
    }

    println("Post Crash")

    println(routerInfo("u1").returnStore)
    println(routerInfo("u2").returnStore)
    println(routerInfo("u3").returnStore)
    println(routerInfo("u4").returnStore)
    println(routerInfo("u5").returnStore)
    println(routerInfo("u6").returnStore)
    println(routerInfo("u7").returnStore)
    println(routerInfo("u8").returnStore)
    */
  }
}