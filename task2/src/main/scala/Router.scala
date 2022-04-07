// Fedor Sergeev
package task2

class Router() {
    /*
     * Class to route messages between nodes.
     */
    var nodes: Map[String, Node] = Map() // Routing information

    def sendMessage(from: String, to: String, message: Message) : Message = {
        /*
         * Sends message to a given node.
         *
         * Parameters
         * ----------
         * from : id of node sending message
         * to : id of destination node
         * message : Message Object
         */
        nodes(to).onReceive(from, message)
    }

    def addNodes(nodeMap: Map[String, Node]) = {
        /*
         * Adds all the routing information from nodeMap to nodes
         */
        nodes ++= nodeMap
    }
}