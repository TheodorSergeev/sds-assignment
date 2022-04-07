// Fedor Sergeev
package task2

class Message(val source: String, val messageType: String, val data: String = "")
/*
 * Class for Messages
 * 
 * Parameters
 * ----------
 * source : id of the node that created the message.
            (Can be used to retain identity when forwarding)
 * messageType : String differentiating between the types of messages.
            (You can invent as many messageTypes as you want)
 * data : String containing the message.
            (Can be left empty when the messageType is enough)
 */