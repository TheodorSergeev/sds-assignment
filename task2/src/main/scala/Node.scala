// Fedor Sergeev
package task2
class Node(val id: String, val memory: Int, private val neighbours: Vector[String], router: Router) {
    /*
     * Class for Node. This is the superclass.
     * You can use all member functions in MyNode.
     * Do not change anything in this class.
     * 
     * Parameters
     * ----------
     * id : id of the node.
     * memory : Total number of (key, value)  pairs the node can store.
                (You must not store over this quota)
     * neighbours : A vector of neighbour ids of the node defined by the overlay network.
     * router : The object of the Router class to be used to send messages to other nodes.
     */
    private val store: scala.collection.mutable.Map[String, String] = scala.collection.mutable.Map() // All keys must be stored here

    def hasMoreSpace() = 
        /*
         * Returns true if at least one more (key, value) pair can be added on this node.
         */
        store.size < memory

    def setKey(key: String, value: String): Boolean = {
        /*
         * Adds the key to the store if possible.
         * Returns true if added, else false.
         * Always use this method when adding (key,value) to the current node.
         */
        if (store.contains(key)) { 
            store(key) = value
            true
        } else if (hasMoreSpace()) { 
            store += (key -> value)
            true
        } else false
    }

    def removeKey(key: String): Boolean = {
        /*
         * Removes the key from the store if possible.
         * Returns true if initally present, else false.
         * Always use this method when removing (key,value) from the current node.
         * The USER will NOT request the removal of the keys.
         */
        if (store.contains(key)) { 
            store -= key
            true
        } else false
    }

    def getKey(key: String): Option[String] = {
        /*
         * Fetches the value of the associated key if present.
         *
         * Returns
         * -------
         * Some(value) if found.
         * None if key not present.
         *
         * Use match and case to differentiate. 
         */
        if (store.contains(key)) Some(store(key)) else None
    }

    def returnStore() = store.toMap
        /*
         * Returns the store as an immutable Map
         */

    def onReceive(from: String, message: Message): Message = {
        /* This method is overridden by MyNode.onReceive */
        ??? /* Don't do anything here */
    }
}