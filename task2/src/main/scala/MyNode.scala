// Fedor Sergeev
package task2

import scala.collection.mutable.Queue
import scala.collection.mutable.Set
import scala.util.control.Breaks._

class MyNode(id: String, memory: Int, neighbours: Vector[String], router: Router) extends Node(id, memory, neighbours, router) {
    val STORE = "STORE"
    val STORE_SUCCESS = "STORE_SUCCESS"
    val STORE_FAILURE = "STORE_FAILURE"
    val RETRIEVE = "RETRIEVE"
    val GET_NEIGHBOURS = "GET_NEIGHBOURS"
    val NEIGHBOURS_RESPONSE = "NEIGHBOURS_RESPONSE"
    val RETRIEVE_SUCCESS = "RETRIEVE_SUCCESS"
    val RETRIEVE_FAILURE = "RETRIEVE_FAILURE"
    val INTERNAL_ERROR = "INTERNAL_ERROR"
    val USER = "USER"
    val nodes_may_fail = 4

    // auxiliary message type for traversal of nodes
    // (used while traversing to request store/retrieve without traversal)
    val BFS_STORE = "BFS_STORE"
    val BFS_RETRIEVE = "BFS_RETRIEVE"

    
    override def onReceive(from: String, message: Message): Message = {
        /* 
         * Called when the node receives a message from some where
         * Feel free to add more methods and code for processing more kinds of messages
         * NOTE: Remember that HOST must still comply with the specifications of USER messages
         *
         * Parameters
         * ----------
         * from: id of node from where the message arrived
         * message: the message
         *           (Process this message and decide what to do)
         *           (All communication between peers happens via these messages)
         */

        /*
         * TODO: task 2.1
         * Add retrieval algorithm to retrieve from the peers here
         * when the key isn't available on the HOST node.
         * Use router.sendMessage(from, to, message) to send a message to another node
         *
         * TODO: task 2.2
         * Change the storage algorithm below to store on the peers
         * when there isn't enough space on the HOST node.
         *
         * TODO: task 2.3
         * Change the storage algorithm below to handle nodes crashing.
         */

        // Request to get the list of neighbours
        if (message.messageType == GET_NEIGHBOURS) {
            new Message(id, NEIGHBOURS_RESPONSE, neighbours.mkString(" "))
        }

        // Request to read the value from only this node (no traversal)
        else if (message.messageType == BFS_RETRIEVE) {
            val key = message.data  // extract key
            val value = getKey(key) // check if the key is present on the node
            var response: Message = new Message("", "", "")

            // if succesfull, send the data
            value match {
                case Some(i) => response = new Message(id, RETRIEVE_SUCCESS, i)
                case None => response = new Message(id, RETRIEVE_FAILURE, neighbours.mkString(","))
            }

            response
        }

        // Request to read the value from the node with further search (traversal)
        else if (message.messageType == RETRIEVE) { 
            val key = message.data  // extract key
            val value = getKey(key) // check if the key is present on the node
            var response: Message = new Message("", "", "")

            value match {
                case Some(i) => response = new Message(id, RETRIEVE_SUCCESS, i)
                case None => response = new Message(id, RETRIEVE_FAILURE)
            }
            
            // if the key is node on the node, traverse the network to search for it
            if (response.messageType == RETRIEVE_FAILURE) {   
                // variables for BFS - set of visited nodes, and a queue of nodes to visit
                var visited_nodes = Set[String]()
                var queued_nodes = Queue[String]()

                visited_nodes.add(id)
                queued_nodes ++= neighbours

                // traverse while we didn't find the node with the value or didn't run out of nodes to visit
                while (response.messageType == RETRIEVE_FAILURE && !queued_nodes.isEmpty) {
                    val new_node = queued_nodes.dequeue()

                    response = router.sendMessage(id, new_node, new Message(id, BFS_RETRIEVE, message.data))

                    if (response.messageType == RETRIEVE_FAILURE) {
                        val new_neighbours = response.data.split(",").toSet
                        // nodes to add the queue
                        // neighbors that were not visited and are not already in the queue
                        // todo: can this be made more efficiently?
                        val new_unvisited_neighbours = new_neighbours diff visited_nodes diff queued_nodes.toSet

                        // add current node to visited, update queue with unvisited neighbours of the current node
                        visited_nodes.add(new_node)
                        queued_nodes ++= new_unvisited_neighbours
                    } else 
                        response = new Message(id, RETRIEVE_SUCCESS, response.data)
                }
            
            }

            response
        }

        // Request to store the value during the network traversal (attempt write, no backups)
        else if (message.messageType == BFS_STORE) {
            val data = message.data.split("->")         // data(0) is key, data(1) is value
            val storedOnSelf = setKey(data(0), data(1)) // try storing on the current node
            val str_neighbours = neighbours.mkString(",")
            
            if (storedOnSelf)
                new Message(id, STORE_SUCCESS, str_neighbours)
            else 
                new Message(id, STORE_FAILURE, str_neighbours)
        } 

        // Request to store the value assuming fault-tolerance
        else if (message.messageType == STORE) {
            val data = message.data.split("->")         // data(0) is key, data(1) is value
            val storedOnSelf = setKey(data(0), data(1)) // try storing on the current node

            var n_backups = nodes_may_fail + 1 // # of writes required for fault-tolerance
            if (storedOnSelf) n_backups -= 1

            // variables for BFS - set of visited nodes, and a queue of nodes to visit
            var visited_nodes = Set[String]()
            var queued_nodes = Queue[String]()

            visited_nodes.add(id)
            queued_nodes ++= neighbours

            // traverse while we didn't make all writes, or didn't run out of nodes to visit
            while (n_backups > 0 && !queued_nodes.isEmpty) {
                val new_node = queued_nodes.dequeue()

                // add current node to visited
                visited_nodes.add(new_node)

                val response = router.sendMessage(id, new_node, new Message(id, BFS_STORE, message.data))
                val new_neighbours = response.data.split(",").toSet

                // nodes to add the queue
                // neighbors that were not visited and are not already in the queue
                // todo: can this be made more efficiently?
                val new_unvisited_neighbours = new_neighbours diff visited_nodes diff queued_nodes.toSet
                
                // update queue with unvisited neighbours of the current node
                queued_nodes ++= new_unvisited_neighbours

                // reduce the number of writes todo if successful
                if (response.messageType == STORE_SUCCESS)
                    n_backups -= 1
            }

            // check task completion
            if (n_backups == 0)
                new Message(id, STORE_SUCCESS) // stored all copies in the network
            else
                new Message(id, STORE_FAILURE) // out of storage for making enough copies (==incorrect querry)
        }

        // Handle unknown requests
        else
            new Message(id, INTERNAL_ERROR)
    }

    def makeBFSMsg(route: String, queue: Queue[String], visited: Set[String], n_backup: Int): String = {
        val str_queued_nodes  = queue .mkString(",")
        val str_visited_nodes = visited.mkString(",")

        route + " " + str_queued_nodes + " " + str_visited_nodes + " " + n_backup.toString
    }

/* My Tesing 

Network graph (replace overlay.txt)
    u1 1 u2 u3
    u2 9 u1 u3 u4
    u3 9 u1 u2 u5 u6
    u4 9 u2
    u5 9 u3
    u6 9 u3

Queries (add to Task2.scala instead of the default queries)
    // Helper function (print current storage of nodes)
    def print_nodes() = {
      val nodes_ids = List[String]("u1", "u2", "u3", "u4", "u5", "u6")
      for (id <- nodes_ids)
        println (id + " " + routerInfo(id).returnStore)
    }

    // Helper function (print message type and data)
    def print_msg(msg: Message) = println(msg.messageType + " " + msg.data)

    // Helper function (crash a node aka delete all store key-value pairs)
    def crash_node(id: String) = {
      var failing_node = routerInfo(id)
      var all_keys = failing_node.returnStore.keySet
      all_keys.foreach { failing_node.removeKey }
    }


    // Check direct store (enough space on in the querried node)
    println("\nCheck direct store")
    var m = router.sendMessage(USER, "u1", new Message(USER, STORE, "key1->value1"))
    print_msg(m)
    print_nodes()

    // Check indirect store (not nough space on in the querried node)
    println("\nCheck indirect store")
    m = router.sendMessage(USER, "u1", new Message(USER, STORE, "key2->value2"))
    print_msg(m)
    print_nodes()

    // Check direct retrieve (value is present in the querried node)
    println("\nCheck direct retrieve")
    m = router.sendMessage(USER, "u1", new Message(USER, RETRIEVE, "key1"))
    print_msg(m)

    // Check indirect retrieve (value is not present in the querried node)
    println("\nCheck indirect retrieve")
    m = router.sendMessage(USER, "u6", new Message(USER, RETRIEVE, "key1"))
    print_msg(m)

    // Simulate crashing 
    println("\nSimulate worst-case crashing")
    crash_node("u1") 
    crash_node("u3") 
    crash_node("u4") 
    crash_node("u5")
    print_nodes()
    m = router.sendMessage(USER, "u4", new Message(USER, RETRIEVE, "key1"))
    print_msg(m)
    m = router.sendMessage(USER, "u4", new Message(USER, RETRIEVE, "key2"))
    print_msg(m)
*/
}