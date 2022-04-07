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
        if (message.messageType == GET_NEIGHBOURS) { // Request to get the list of neighbours
            new Message(id, NEIGHBOURS_RESPONSE, neighbours.mkString(" "))
        }
        else if (message.messageType == RETRIEVE) { // Request to get the value
            val key = message.data // This is the key
            val value = getKey(key) // Check if the key is present on the node
            var response : Message = new Message("", "", "")
            value match {
                case Some(i) => response = new Message(id, RETRIEVE_SUCCESS, i)
                case None => response = new Message(id, RETRIEVE_FAILURE)
            }
            /*
             * TODO: task 2.1
             * Add retrieval algorithm to retrieve from the peers here
             * when the key isn't available on the HOST node.
             * Use router.sendMessage(from, to, message) to send a message to another node
             */
            if (response.messageType == RETRIEVE_FAILURE)
                for (neighbour_id <- neighbours)
                    if (response.messageType == RETRIEVE_FAILURE && neighbour_id != from) {
                        response = router.sendMessage(id, neighbour_id, message)
                        //println(neighbour_id + " " + response.messageType)
                    }

            response // Return the correct response message
        }
        else if (message.messageType == STORE) { // Request to store key->value
            /*
             * TODO: task 2.2
             * Change the storage algorithm below to store on the peers
             * when there isn't enough space on the HOST node.
             *
             * TODO: task 2.3
             * Change the storage algorithm below to handle nodes crashing.
             */
            val data = message.data.split("->") // data(0) is key, data(1) is value
            val storedOnSelf = setKey(data(0), data(1)) // Store on current node

            if (storedOnSelf) {
                new Message(id, STORE_SUCCESS)
            }
            else {
                for (neighbour_id <- neighbours)
                    if (response.messageType == STORE_FAILURE && neighbour_id != from) {
                        response = router.sendMessage(id, neighbour_id, message)
                    }

                new Message(id, STORE_FAILURE)
            }
        }
        /*
         * Feel free to add more kinds of messages.
         */
        else
            new Message(id, INTERNAL_ERROR)
    }
}