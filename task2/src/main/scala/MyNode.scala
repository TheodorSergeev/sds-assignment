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

    val BFS_STORE = "BFS_STORE"
    val BFS_STORE_SUCCESS = "BFS_STORE_SUCCESS"
    val BFS_STORE_FAILURE = "BFS_STORE_FAILURE"

    
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

        // Request to get the value
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
                for (neighbour_id <- neighbours)
                    if (response.messageType == RETRIEVE_FAILURE && neighbour_id != from) {
                        response = router.sendMessage(id, neighbour_id, message)
                        //println(neighbour_id + " " + response.messageType)
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
                new Message(id, BFS_STORE_SUCCESS, str_neighbours)
            else 
                new Message(id, BFS_STORE_FAILURE, str_neighbours)
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

                val response = router.sendMessage(id, new_node, new Message(id, BFS_STORE, message.data))
                val new_neighbours = response.data.split(",") 
                val new_unvisited_neighbours = new_neighbours.toSet diff visited_nodes

                // add current node to visited, update queue with unvisited neighbours of the current node
                visited_nodes.add(new_node)
                queued_nodes ++= new_unvisited_neighbours

                // reduce the number of writes todo if successful
                if (response.messageType == BFS_STORE_SUCCESS)
                    n_backups -= 1
                else if (response.messageType == BFS_STORE_FAILURE)
                    n_backups -= 0
                else 
                    new Message(id, INTERNAL_ERROR)
            }

            // check task completion
            if (n_backups == 0)
                new Message(id, STORE_SUCCESS) // stored all copies in the network
            else
                new Message(id, STORE_FAILURE) // out of storage for making enough copies
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

    /*
    def onStore(from: String, message: Message) = {
        // Implement BFS

        // Message format: 
        //   <source>-><destination> <queue of nodes to visit separated by commas> <set of visited nodes> <backups to make>
        // Example:
        //   u1->u2 u1,u2,u3,u4 u5,u6,u7 3

        val data = message.data.split(" ")

        val routing = data(0).split("->")
        val queued_nodes = Queue(data(1).split(","))
        val visited_nodes = Set(data(2).split(","))

        var n_backups = data(2).toInt

        // try storing on the current node
        val storedOnSelf = setKey(routing(0), routing(1)) 

        // decrease todo counter if the write was possible b
        if (storedOnSelf)
            backup_num -= 1
            
        // if we made all writes that were required => success
        if (backup_num == 0)
            new Message(id, STORE_SUCCESS)
        
        else
            // this node has been visited, so add it to the set
            visited_nodes.add(id)
            // add neighbors that have not yet been visited to the queue
            queued_nodes += neighbours diff visited_nodes

            // no more nodes for visit, but more storage is needed => failure
            if (queued_nodes.isEmpty)
                new Message(id, STORE_SUCCESS)

            // there are more nodes to visit => do next iteration of BFS
            else                
                println("bfs magic")
    }
    */
}