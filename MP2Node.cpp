/**********************************
 * FILE NAME: MP2Node.cpp
 *
 * DESCRIPTION: MP2Node class definition
 **********************************/
#include <string.h>
#include "MP2Node.h"


/*
 * Global variables to store reply response count 
 */

int numSuccessReply[MAX_G_TRANS] = { [0 ... MAX_G_TRANS - 1] = 0 };;
int numFailReply[MAX_G_TRANS] = { [0 ... MAX_G_TRANS - 1] = 0 };;

/**
 * constructor
 */
MP2Node::MP2Node(Member *memberNode, Params *par, EmulNet * emulNet, Log * log, Address * address) {
	this->memberNode = memberNode;
	this->par = par;
	this->emulNet = emulNet;
	this->log = log;
	ht = new HashTable();
	this->memberNode->addr = *address;
}

/**
 * Destructor
 */
MP2Node::~MP2Node() {
	delete ht;
	delete memberNode;
}

/**
 * FUNCTION NAME: updateRing
 *
 * DESCRIPTION: This function does the following:
 * 				1) Gets the current membership list from the Membership Protocol (MP1Node)
 * 				   The membership list is returned as a vector of Nodes. See Node class in Node.h
 * 				2) Constructs the ring based on the membership list
 * 				3) Calls the Stabilization Protocol
 */
void MP2Node::updateRing() {
	/*
	 * Implement this. Parts of it are already implemented
	 */
	vector<Node> curMemList;
	bool change = false;

	vector<Node>::iterator it;
	/*
	 *  Step 1. Get the current membership list from Membership Protocol / MP1
	 */
	curMemList = getMembershipList();

	/*
	 * Step 2: Construct the ring
	 */
	// Sort the list based on the hashCode
	sort(curMemList.begin(), curMemList.end());


	/*
	 * Step 3: Run the stabilization protocol IF REQUIRED
	 */
	// Run stabilization protocol if the hash table size is greater than zero and if there has been a changed in the ring

	cout << "Called updateRing() , curMemList size is " << curMemList.size() << endl;

	// Check for Ring table size 
	// if it is zero, initialize the ring.
	cout << "I am Node " << this->memberNode->addr.getAddress();
	cout << " My Ring size is " << this->ring.size() << endl;

	if (this->ring.size() == 0) {
		// Initialize the ring. Since we are initializing the ring, the node is just joining for the first time
		// hashtable is going to be empty

		cout << "My Ring is empty " << endl;
		it = this->ring.begin();
		this->ring.insert(it, curMemList.begin(), curMemList.end());

		cout << "Ring size is " << this->ring.size() << endl;

		// Identify neighbors 

		this->findNeighbors();

	}

	// If Memberlist table size is different than current hash table size 
	// it means a node has joined or failed, time to update the ring table

	if ( this->ring.size() != curMemList.size() ) {
		cout << "Membership size has changed, Ring table has changed" << endl;
		cout << "My current Ring size is " << this->ring.size() << endl;
		this->ring.clear();
		it = this->ring.begin();
		this->ring.insert(it, curMemList.begin(), curMemList.end());
		cout << "Ring size is " << this->ring.size() << endl;
		this->findNeighbors();
		if (!this->ht->isEmpty()){
			cout << "Time to call the stabilization protocol" << endl;

		}
	}



}


/**
 * FUNCTION NAME: findNeighbors()
 * DESCRIPTION : Identify you ownrself in the Ring
 *               Pick next two nodes in the ring as successor 
 *               Save information of these two nodes in the <hasmyReplica> 
 *               Identify two predecessor nodes 
 *               Save information of these two nodes in the  <haveReplicasOf>
 */

void MP2Node::findNeighbors() {
	int selfIndex = 0;
	int firstSuccessor = 0;
	int secondSuccessor = 0;
	int firstPredecessor = 0;
	int secondPredecessor = 0;
	int ringSize = this->ring.size();
	vector<Node>::iterator it;

	for (unsigned int i = 0; i < ringSize; i++) {
		if (this->ring[i].nodeAddress.getAddress().compare(this->memberNode->addr.getAddress()) == 0) {
			// We have found the index of current node in the sorted ring list 
			selfIndex = i;
			break;
		}
	}


	if (selfIndex + 1 > ringSize) {
		firstSuccessor = selfIndex + 1 - ringSize;
	}
	else {
		firstSuccessor = selfIndex + 1;
	}

	if (selfIndex + 2 > ringSize) {
		secondSuccessor = selfIndex + 2 - ringSize;
	}
	else {
		secondSuccessor = selfIndex + 2;
	}
	
	if (selfIndex - 1 < 0 ) {
		firstPredecessor = selfIndex - 1 + ringSize;
	}
	else {
		firstPredecessor = selfIndex - 1;
	}

	if (selfIndex - 2 < 0 ) {
		secondPredecessor = selfIndex - 2 + ringSize;
	}
	else {
		secondPredecessor = selfIndex - 2;
	}

	this->hasMyReplicas.clear();
	this->hasMyReplicas.emplace_back(this->ring[firstSuccessor]);
	this->hasMyReplicas.emplace_back(this->ring[secondSuccessor]);
	
	this->haveReplicasOf.clear();
	this->haveReplicasOf.emplace_back(this->ring[firstPredecessor]);
	this->haveReplicasOf.emplace_back(this->ring[secondPredecessor]);

	cout << "Self Index is " << selfIndex 
		 << " ,First Successor is " << firstSuccessor 
		 << " ,Second Successor is " << secondSuccessor 
		 << " ,First predecessor is " << firstPredecessor
		 << " ,Second predecessor is " << secondPredecessor 
		 << " ,Successor vector size is " << this->hasMyReplicas.size() 
		 << " ,Predecessor vector size is " << this->haveReplicasOf.size()
		 << endl;


}

/**
 * FUNCTION NAME: getMemberhipList
 *
 * DESCRIPTION: This function goes through the membership list from the Membership protocol/MP1 and
 * 				i) generates the hash code for each member
 * 				ii) populates the ring member in MP2Node class
 * 				It returns a vector of Nodes. Each element in the vector contain the following fields:
 * 				a) Address of the node
 * 				b) Hash code obtained by consistent hashing of the Address
 */
vector<Node> MP2Node::getMembershipList() {
	unsigned int i;
	vector<Node> curMemList;
	for ( i = 0 ; i < this->memberNode->memberList.size(); i++ ) {
		Address addressOfThisMember;
		int id = this->memberNode->memberList.at(i).getid();
		short port = this->memberNode->memberList.at(i).getport();
		memcpy(&addressOfThisMember.addr[0], &id, sizeof(int));
		memcpy(&addressOfThisMember.addr[4], &port, sizeof(short));
		curMemList.emplace_back(Node(addressOfThisMember));
	}
	return curMemList;
}

/**
 * FUNCTION NAME: hashFunction
 *
 * DESCRIPTION: This functions hashes the key and returns the position on the ring
 * 				HASH FUNCTION USED FOR CONSISTENT HASHING
 *
 * RETURNS:
 * size_t position on the ring
 */
size_t MP2Node::hashFunction(string key) {
	std::hash<string> hashFunc;
	size_t ret = hashFunc(key);
	return ret%RING_SIZE;
}

/**
 * FUNCTION NAME: clientCreate
 *
 * DESCRIPTION: client side CREATE API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientCreate(string key, string value) {
	/*
	 * Implement this
	 */

	vector<Node> replicaCreate;

	Message clientCreate(g_transID++, this->memberNode->addr, CREATE, key, value);

	cout << endl << "Node "  << this->memberNode->addr.getAddress() 
		 << " received a client create message with " << key 
		 << " and " << value <<endl;


	// call findNodes() to identify primary, secondary and tertiary replica nodes for the given key

	replicaCreate = this->findNodes(key);

	cout << "Primary Replica HashCode is " << replicaCreate[0].getHashCode() << endl;
	cout << "Secondary Replica HashCode is " << replicaCreate[1].getHashCode() << endl;
	cout << "Tertiary Replica HashCode is " << replicaCreate[2].getHashCode() << endl;

	// send messages to the primary replica

	clientCreate.replica = PRIMARY;

	this->emulNet->ENsend(&this->memberNode->addr, replicaCreate[0].getAddress(), 
	 				 (char *)&clientCreate, sizeof(Message));

	// send message to both secondary and tertiary replicas

	clientCreate.replica = SECONDARY;

	this->emulNet->ENsend(&this->memberNode->addr, replicaCreate[1].getAddress(), 
	 				 (char *)&clientCreate, sizeof(Message));

	this->emulNet->ENsend(&this->memberNode->addr, replicaCreate[2].getAddress(), 
	 				 (char *)&clientCreate, sizeof(Message));



}

/**
 * FUNCTION NAME: clientRead
 *
 * DESCRIPTION: client side READ API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientRead(string key){
	/*
	 * Implement this
	 */

	vector<Node> replicaRead;

	Message clientRead(g_transID++, this->memberNode->addr, READ, key);

	cout << endl << "Node "  << this->memberNode->addr.getAddress() 
		 << " received a client read message with " << key <<endl;


	// call findNodes() to identify primary, secondary and tertiary replica nodes for the given key

	replicaRead = this->findNodes(key);

	cout << "Primary Replica HashCode is " << replicaRead[0].getHashCode() << endl;
	cout << "Secondary Replica HashCode is " << replicaRead[1].getHashCode() << endl;
	cout << "Tertiary Replica HashCode is " << replicaRead[2].getHashCode() << endl;

	// send messages to the primary replica

	clientRead.replica = PRIMARY;

	this->emulNet->ENsend(&this->memberNode->addr, replicaRead[0].getAddress(), 
	 				 (char *)&clientRead, sizeof(Message));

	// send message to both secondary and tertiary replicas

	clientRead.replica = SECONDARY;

	this->emulNet->ENsend(&this->memberNode->addr, replicaRead[1].getAddress(), 
	 				 (char *)&clientRead, sizeof(Message));

	this->emulNet->ENsend(&this->memberNode->addr, replicaRead[2].getAddress(), 
	 				 (char *)&clientRead, sizeof(Message));




}

/**
 * FUNCTION NAME: clientUpdate
 *
 * DESCRIPTION: client side UPDATE API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientUpdate(string key, string value){
	/*
	 * Implement this
	 */
}

/**
 * FUNCTION NAME: clientDelete
 *
 * DESCRIPTION: client side DELETE API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientDelete(string key){
	/*
	 * Implement this
	 */


	vector<Node> replicaDelete;

	Message clientDelete(g_transID++, this->memberNode->addr, DELETE, key);

	cout << endl << "Node "  << this->memberNode->addr.getAddress() 
		 << " received a client delete message for " << key <<endl;


	// call findNodes() to identify primary, secondary and tertiary replica nodes for the given key

	replicaDelete = this->findNodes(key);

	cout << "Primary Replica HashCode is " << replicaDelete[0].getHashCode() << endl;
	cout << "Secondary Replica HashCode is " << replicaDelete[1].getHashCode() << endl;
	cout << "Tertiary Replica HashCode is " << replicaDelete[2].getHashCode() << endl;

	// send messages to the primary replica

	clientDelete.replica = PRIMARY;

	this->emulNet->ENsend(&this->memberNode->addr, replicaDelete[0].getAddress(), 
	 				 (char *)&clientDelete, sizeof(Message));

	// send message to both secondary and tertiary replicas

	clientDelete.replica = SECONDARY;

	this->emulNet->ENsend(&this->memberNode->addr, replicaDelete[1].getAddress(), 
	 				 (char *)&clientDelete, sizeof(Message));

	this->emulNet->ENsend(&this->memberNode->addr, replicaDelete[2].getAddress(), 
	 				 (char *)&clientDelete, sizeof(Message));

}

/**
 * FUNCTION NAME: createKeyValue
 *
 * DESCRIPTION: Server side CREATE API
 * 			   	The function does the following:
 * 			   	1) Inserts key value into the local hash table
 * 			   	2) Return true or false based on success or failure
 */
bool MP2Node::createKeyValue(string key, string value, ReplicaType replica) {
	/*
	 * Implement this
	 */
	// Insert key, value, replicaType into the hash table
	bool retVal;

	retVal = this->ht->create(key, value);

	return retVal;

}

/**
 * FUNCTION NAME: readKey
 *
 * DESCRIPTION: Server side READ API
 * 			    This function does the following:
 * 			    1) Read key from local hash table
 * 			    2) Return value
 */
string MP2Node::readKey(string key) {
	/*
	 * Implement this
	 */
	// Read key from local hash table and return value

	string retVal;

	retVal = this->ht->read(key);

	return retVal;

}

/**
 * FUNCTION NAME: updateKeyValue
 *
 * DESCRIPTION: Server side UPDATE API
 * 				This function does the following:
 * 				1) Update the key to the new value in the local hash table
 * 				2) Return true or false based on success or failure
 */
bool MP2Node::updateKeyValue(string key, string value, ReplicaType replica) {
	/*
	 * Implement this
	 */
	// Update key in local hash table and return true or false
	

}

/**
 * FUNCTION NAME: deleteKey
 *
 * DESCRIPTION: Server side DELETE API
 * 				This function does the following:
 * 				1) Delete the key from the local hash table
 * 				2) Return true or false based on success or failure
 */
bool MP2Node::deletekey(string key) {
	/*
	 * Implement this
	 */
	// Delete the key from the local hash table

	bool retVal;

	retVal = this->ht->deleteKey(key);

	return retVal;
}


/**
 * FUNCTION NAME: createReply
 *
 * DESCRIPTION: This function sends a Reply message in response to create or delete.
 * 				This function does the following:
 * 				1) Creates a REPLY message
 * 				2) Sends it back to the requestor
 */
void MP2Node::createReply(int transID, Address *toAddr, MessageType type, 
							bool result, string key, string value, ReplicaType replica) {

	Message createReply(transID, this->memberNode->addr, type, result);

	createReply.key = key;
	createReply.value = value;
	createReply.replica = replica;

	//int ENsend(Address *myaddr, Address *toaddr, char *data, int size);

	this->emulNet->ENsend(&this->memberNode->addr, toAddr, (char *)&createReply, sizeof(Message));

}


/**
 * FUNCTION NAME: readReply
 *
 * DESCRIPTION: This function sends a ReadReply message in response to read request.
 * 				This function does the following:
 * 				1) Creates a READREPLY message
 * 				2) Sends it back to the requestor
 */
void MP2Node::readReply(int transID, Address *toAddr, MessageType type, 
							bool result, string key, string value) {

	// construct read reply message
	// Message(int _transID, Address _fromAddr, string _value);
	Message readReply(transID, this->memberNode->addr, value);

	readReply.key = key;
	readReply.success = result;

	this->emulNet->ENsend(&this->memberNode->addr, toAddr, (char *)&readReply, sizeof(Message));

}


/**
 * FUNCTION NAME: checkMessages
 *
 * DESCRIPTION: This function is the message handler of this node.
 * 				This function does the following:
 * 				1) Pops messages from the queue
 * 				2) Handles the messages according to message types
 */
void MP2Node::checkMessages() {
	/*
	 * Implement this. Parts of it are already implemented
	 */
	char * data;
	int size;

	/*
	 * Declare your local variables here
	 */

	Message *recvdMsg;
	Message *recvdReplyMsg;
	bool result;
	string readValue;
	const char * returnedOp;


	// dequeue all messages and handle them
	while ( !memberNode->mp2q.empty() ) {
		/*
		 * Pop a message from the queue
		 */

		data = (char *)memberNode->mp2q.front().elt;
		size = memberNode->mp2q.front().size;
		memberNode->mp2q.pop();

		string message(data, data + size);

		recvdMsg = (Message *)data;

		/*
		 * Handle the message types here
		 */

		cout << "Node " << this->memberNode->addr.getAddress();
		cout << " Msg Key " << recvdMsg->key << " with value " << recvdMsg->value << endl;

		switch(recvdMsg->type) {
			case CREATE:
				cout << " recvd a CREATE of type " << recvdMsg->type << endl;
				result = this->createKeyValue(recvdMsg->key, recvdMsg->value, recvdMsg->replica);
				
				// log the result of keyvalue create operation

				if (result) {
					log->logCreateSuccess(&this->memberNode->addr, false, recvdMsg->transID, 
											recvdMsg->key, recvdMsg->value);
				}
				else {
					log->logCreateFail(&this->memberNode->addr, false, recvdMsg->transID, 
											recvdMsg->key, recvdMsg->value);
				}

				// send a reply message to coordinator 
				// with the result of the create entry operations using same transID

				
				this->createReply(recvdMsg->transID, &recvdMsg->fromAddr, 
									REPLY, result, recvdMsg->key, recvdMsg->value, recvdMsg->replica);

				break;
			case READ:
				cout << " recvd a READ of type " << recvdMsg->type << endl;

				readValue = this->readKey(recvdMsg->key);
				
				// log the result of keyvalue create operation

				if (readValue != "") {
					log->logReadSuccess(&this->memberNode->addr, false, recvdMsg->transID, 
											recvdMsg->key, readValue);
					result = true;
				}
				else {
					log->logReadFail(&this->memberNode->addr, false, recvdMsg->transID, 
											recvdMsg->key);
					result = false;
				}

				// send a reply message to coordinator 
				// with the result of the create entry operations using same transID

				this->readReply(recvdMsg->transID, &recvdMsg->fromAddr, 
									READREPLY, result, recvdMsg->key, readValue);

				break;				


				break;
			case UPDATE:
				cout << " recvd a UPDATE of type " << recvdMsg->type << endl;
				break;
			case DELETE:
				cout << " recvd a DELETE of type " << recvdMsg->type << endl;
				result = this->deletekey(recvdMsg->key);
				
				// log the result of keyvalue create operation

				if (result) {
					log->logDeleteSuccess(&this->memberNode->addr, false, recvdMsg->transID, 
											recvdMsg->key);
				}
				else {
					log->logDeleteFail(&this->memberNode->addr, false, recvdMsg->transID, 
											recvdMsg->key);
				}

				// send a reply message to coordinator 
				// with the result of the create entry operations using same transID
				// in case of delete set the value field to DELETED
				// coordinate will know that this is a reply for a DELETE message and log
				// the operation result accordingly

				
				this->createReply(recvdMsg->transID, &recvdMsg->fromAddr, 
									REPLY, result, recvdMsg->key, "DELETED", recvdMsg->replica);


				break;


			case REPLY:
				// extract c string version of the value returned in the msg 
				// this is necessary to do  strcmp
				returnedOp = recvdMsg->value.c_str();
				cout << " recvd a REPLY of type " << recvdMsg->type << " for transID " << recvdMsg->transID << endl;
				// Coordinator receives a reply from all replicas
				// if all three replicas replied with success, log success with coordinator flag enabled

				if (recvdMsg->success == true) {
					numSuccessReply[recvdMsg->transID]++;
				} 
				else if (recvdMsg->success == false) {
					numFailReply[recvdMsg->transID]++;
				}	

				// if all 2 out of 3 replicas respond with success 
				// coordinate mark the operation as success
				// otherwise marks the operation as a failure
				// reset response counters for success and failures

				cout << "Success Replies = " << this->numSuccessReply[recvdMsg->transID] 
					 << " Fail Replies = " << this->numFailReply[recvdMsg->transID] << endl;	 

				if (numSuccessReply[recvdMsg->transID] + numFailReply[recvdMsg->transID] == REPLICA_QUORUM + 1) {
					if ((this->numSuccessReply[recvdMsg->transID] >= REPLICA_QUORUM) && 
						(this->numFailReply[recvdMsg->transID] < REPLICA_QUORUM)) {
						if (strcmp(returnedOp, "DELETED") == 0) {
						    log->logDeleteSuccess(&this->memberNode->addr, true, recvdMsg->transID, recvdMsg->key);							

						}
						else {
						    log->logCreateSuccess(&this->memberNode->addr, true, recvdMsg->transID, 
											recvdMsg->key, recvdMsg->value);							
						}

						this->numSuccessReply[recvdMsg->transID] = 0;
						this->numFailReply[recvdMsg->transID] = 0;
					}
					else  {
						if (strcmp(returnedOp, "DELETED") == 0) {
						    log->logDeleteFail(&this->memberNode->addr, true, recvdMsg->transID, recvdMsg->key);							

						}
						else {
							log->logCreateFail(&this->memberNode->addr, true, recvdMsg->transID, 
											recvdMsg->key, recvdMsg->value);
						}	
						this->numSuccessReply[recvdMsg->transID]=0;
						this->numFailReply[recvdMsg->transID] = 0;
					}
				}
				
				break;

			case READREPLY:

				/*
					mp2[nodesToFail.at(i)]->getMemberNode()->bFailed = true;
					mp1[nodesToFail.at(i)]->getMemberNode()->bFailed = true;
				*/	
				cout << " recvd a READREPLY of type " << recvdMsg->type << endl;

				if (recvdMsg->success == true) {
					numSuccessReply[recvdMsg->transID]++;
				} 
				else if (recvdMsg->success == false) {
					numFailReply[recvdMsg->transID]++;
				}

				cout << "Success Replies (READREPLY) = " << this->numSuccessReply[recvdMsg->transID] 
					 << " Fail Replies (READREPLY) = " << this->numFailReply[recvdMsg->transID] << endl;	 

				if (numSuccessReply[recvdMsg->transID] + numFailReply[recvdMsg->transID] == REPLICA_QUORUM + 1) {
					if ((this->numSuccessReply[recvdMsg->transID] >= REPLICA_QUORUM) && 
						(this->numFailReply[recvdMsg->transID] < REPLICA_QUORUM)) {

						log->logReadSuccess(&this->memberNode->addr, true, recvdMsg->transID, recvdMsg->key, recvdMsg->value);
						this->numSuccessReply[recvdMsg->transID] = 0;
						this->numFailReply[recvdMsg->transID] = 0;							
					}
					else if ((this->numSuccessReply[recvdMsg->transID] < REPLICA_QUORUM) && 
						(this->numFailReply[recvdMsg->transID] >= REPLICA_QUORUM)) {
						log->logReadFail(&this->memberNode->addr, true, recvdMsg->transID, recvdMsg->key);							
						this->numSuccessReply[recvdMsg->transID] = 0;
						this->numFailReply[recvdMsg->transID] = 0;
					}
					else {
						log->logReadFail(&this->memberNode->addr, true, recvdMsg->transID, recvdMsg->key);							
						this->numSuccessReply[recvdMsg->transID] = 0;
						this->numFailReply[recvdMsg->transID] = 0;	
					}
				} 
				else if (numSuccessReply[recvdMsg->transID] == REPLICA_QUORUM) {
					log->logReadSuccess(&this->memberNode->addr, true, recvdMsg->transID, recvdMsg->key, recvdMsg->value);
						this->numSuccessReply[recvdMsg->transID] = 0;
						this->numFailReply[recvdMsg->transID] = 0;
				}





				break;

			default:
				break;		

		}
 
		switch(recvdMsg->replica) {
			case PRIMARY:
				cout << " PRIMARY REPLICA NODE " << endl;
				break;
			case SECONDARY:
				cout << " SECONDARY REPLICA NODE "  << endl;
				break;
			case TERTIARY:
				cout << " TERTIARY REPLICA NODE "  << endl;
				break;
			default:
				break;	

		}

	}

	/*
	 * This function should also ensure all READ and UPDATE operation
	 * get QUORUM replies
	 */


}

/**
 * FUNCTION NAME: findNodes
 *
 * DESCRIPTION: Find the replicas of the given keyfunction
 * 				This function is responsible for finding the replicas of a key
 */
vector<Node> MP2Node::findNodes(string key) {
	size_t pos = hashFunction(key);
	vector<Node> addr_vec;
	if (ring.size() >= 3) {
		// if pos <= min || pos > max, the leader is the min
		if (pos <= ring.at(0).getHashCode() || pos > ring.at(ring.size()-1).getHashCode()) {
			addr_vec.emplace_back(ring.at(0));
			addr_vec.emplace_back(ring.at(1));
			addr_vec.emplace_back(ring.at(2));
		}
		else {
			// go through the ring until pos <= node
			for (int i=1; i<ring.size(); i++){
				Node addr = ring.at(i);
				if (pos <= addr.getHashCode()) {
					addr_vec.emplace_back(addr);
					addr_vec.emplace_back(ring.at((i+1)%ring.size()));
					addr_vec.emplace_back(ring.at((i+2)%ring.size()));
					break;
				}
			}
		}
	}
	return addr_vec;
}

/**
 * FUNCTION NAME: recvLoop
 *
 * DESCRIPTION: Receive messages from EmulNet and push into the queue (mp2q)
 */
bool MP2Node::recvLoop() {
    if ( memberNode->bFailed ) {
    	return false;
    }
    else {
    	return emulNet->ENrecv(&(memberNode->addr), this->enqueueWrapper, NULL, 1, &(memberNode->mp2q));
    }
}

/**
 * FUNCTION NAME: enqueueWrapper
 *
 * DESCRIPTION: Enqueue the message from Emulnet into the queue of MP2Node
 */
int MP2Node::enqueueWrapper(void *env, char *buff, int size) {
	Queue q;
	return q.enqueue((queue<q_elt> *)env, (void *)buff, size);
}
/**
 * FUNCTION NAME: stabilizationProtocol
 *
 * DESCRIPTION: This runs the stabilization protocol in case of Node joins and leaves
 * 				It ensures that there always 3 copies of all keys in the DHT at all times
 * 				The function does the following:
 *				1) Ensures that there are three "CORRECT" replicas of all the keys in spite of failures and joins
 *				Note:- "CORRECT" replicas implies that every key is replicated in its two neighboring nodes in the ring
 */
void MP2Node::stabilizationProtocol() {
	/*
	 * Implement this
	 */

	

	if (this->ht->isEmpty() == true) {
		// do nothing if hash table is empty
		// purely defensive check
		return;
	}

	// get current size of hash table




}
