/**********************************
 * FILE NAME: MP1Node.cpp
 *
 * DESCRIPTION: Membership protocol run by this Node.
 * 				Definition of MP1Node class functions.
 **********************************/

#include "MP1Node.h"
#include <iostream>
#include <sys/time.h>
#include "Member.h"
#include <algorithm>
/*
 * Note: You can change/add any functions in MP1Node.{h,cpp}
 */

/**
 * Overloaded Constructor of the MP1Node class
 * You can add new members to the class if you think it
 * is necessary for your logic to work
 */
MP1Node::MP1Node(Member *member, Params *params, EmulNet *emul, Log *log, Address *address) {
	for( int i = 0; i < 6; i++ ) {
		NULLADDR[i] = 0;
	}
	this->memberNode = member;
	this->emulNet = emul;
	this->log = log;
	this->par = params;
	this->memberNode->addr = *address;
}

/**
 * Destructor of the MP1Node class
 */
MP1Node::~MP1Node() {}

/**
 * FUNCTION NAME: recvLoop
 *
 * DESCRIPTION: This function receives message from the network and pushes into the queue
 * 				This function is called by a node to receive messages currently waiting for it
 */
int MP1Node::recvLoop() {
    if ( memberNode->bFailed ) {
    	return false;
    }
    else {
    	return emulNet->ENrecv(&(memberNode->addr), enqueueWrapper, NULL, 1, &(memberNode->mp1q));
    }
}

/**
 * FUNCTION NAME: enqueueWrapper
 *
 * DESCRIPTION: Enqueue the message from Emulnet into the queue
 */
int MP1Node::enqueueWrapper(void *env, char *buff, int size) {
	Queue q;
	return q.enqueue((queue<q_elt> *)env, (void *)buff, size);
}

/**
 * FUNCTION NAME: nodeStart
 *
 * DESCRIPTION: This function bootstraps the node
 * 				All initializations routines for a member.
 * 				Called by the application layer.
 */
void MP1Node::nodeStart(char *servaddrstr, short servport) {
    Address joinaddr;
    joinaddr = getJoinAddress();

    // Self booting routines
    if( initThisNode(&joinaddr) == -1 ) {
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "init_thisnode failed. Exit.");
#endif
        exit(1);
    }

    if( !introduceSelfToGroup(&joinaddr) ) {
        finishUpThisNode();
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "Unable to join self to group. Exiting.");
#endif
        exit(1);
    }

    return;
}

/**
 * FUNCTION NAME: initThisNode
 *
 * DESCRIPTION: Find out who I am and start up
 */
int MP1Node::initThisNode(Address *joinaddr) {
	/*
	 * This function is partially implemented and may require changes
	 */
	int id = *(int*)(&memberNode->addr.addr);
	int port = *(short*)(&memberNode->addr.addr[4]);
    MemberListEntry initMember;    

	memberNode->bFailed = false;
	memberNode->inited = true;
	memberNode->inGroup = false;
    // node is up!
	memberNode->nnb = 0;
	memberNode->heartbeat = 0;
	memberNode->pingCounter = TFAIL;
	memberNode->timeOutCounter = -1;
    initMemberListTable(memberNode);


    /* if this node is the initiator, add self entry in the membership list table */

    if ( 0 == memcmp((char *)&(memberNode->addr.addr), (char *)&(joinaddr->addr), sizeof(memberNode->addr.addr))) {
        cout << "Initializing the INIT node" << endl;
        initMember.setid(id);
        initMember.setport(port);
        initMember.setheartbeat(memberNode->heartbeat);
        initMember.settimestamp(par->getcurrtime());
        memberNode->memberList.push_back(initMember);

    }
    return 0;
}

/**
 * FUNCTION NAME: introduceSelfToGroup
 *
 * DESCRIPTION: Join the distributed system
 */
int MP1Node::introduceSelfToGroup(Address *joinaddr) {
	MessageHdr *msg;
#ifdef DEBUGLOG
    static char s[1024];
#endif

    if ( 0 == memcmp((char *)&(memberNode->addr.addr), (char *)&(joinaddr->addr), sizeof(memberNode->addr.addr))) {
        // I am the group booter (first process to join the group). Boot up the group
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "Starting up group...");
#endif
        memberNode->inGroup = true;
    }
    else {
        size_t msgsize = sizeof(MessageHdr) + sizeof(joinaddr->addr) + sizeof(long) + 1;
        msg = (MessageHdr *) malloc(msgsize * sizeof(char));

        // create JOINREQ message: format of data is {struct Address myaddr}
        msg->msgType = JOINREQ;
        memcpy((char *)(msg+1), &memberNode->addr.addr, sizeof(memberNode->addr.addr));
        memcpy((char *)(msg+1) + 1 + sizeof(memberNode->addr.addr), &memberNode->heartbeat, sizeof(long));

#ifdef DEBUGLOG
        sprintf(s, "Trying to join...");
        log->LOG(&memberNode->addr, s);
#endif

        // send JOINREQ message to introducer member
        emulNet->ENsend(&memberNode->addr, joinaddr, (char *)msg, msgsize);

        free(msg);
    }

    return 1;

}

/**
 * FUNCTION NAME: finishUpThisNode
 *
 * DESCRIPTION: Wind up this node and clean up state
 */
int MP1Node::finishUpThisNode(){
   /*
    * Your code goes here
    */
#ifdef DEBUGLOG
    static char s[1024];
#endif
    emulNet->ENcleanup();
    sprintf(s, "Cleaning up by calling ENCleanup...\n");
    log->LOG(&memberNode->addr, s);

    return 0;    
}

/**
 * FUNCTION NAME: nodeLoop
 *
 * DESCRIPTION: Executed periodically at each member
 * 				Check your messages in queue and perform membership protocol duties
 */
void MP1Node::nodeLoop() {
    if (memberNode->bFailed) {
    	return;
    }

    // Check my messages
    checkMessages();

    // Wait until you're in the group...
    if( !memberNode->inGroup ) {
    	return;
    }

    // ...then jump in and share your responsibilites!
    nodeLoopOps();

    return;
}

/**
 * FUNCTION NAME: checkMessages
 *
 * DESCRIPTION: Check messages in the queue and call the respective message handler
 */
void MP1Node::checkMessages() {
    void *ptr;
    int size;

    // Pop waiting messages from memberNode's mp1q
    while ( !memberNode->mp1q.empty() ) {
    	ptr = memberNode->mp1q.front().elt;
    	size = memberNode->mp1q.front().size;
    	memberNode->mp1q.pop();
    	recvCallBack((void *)memberNode, (char *)ptr, size);
    }
    return;
}

/**
 * FUNCTION NAME: recvCallBack
 *
 * DESCRIPTION: Message handler for different message types
 */
bool MP1Node::recvCallBack(void *env, char *data, int size ) {
	/*
	 * Your code goes here
	 */
     MemberListEntry recvdMember;

#ifdef DEBUGLOG
    /* static char s[1024]; */
#endif
    Member *self = (Member *)env;
    /* char *data2 = (char *)env; */
    
    MsgTypes recvdMsgType = *(MsgTypes *)&data[0];

    char *sender = (data + sizeof(MessageHdr));

    long *recvdHeartbeat = (long *)(data + sizeof(MessageHdr) + sizeof(Address) + 1);
    short recvdPort;
    int recvdId;
    int memberTableSize  = 0;
    int entrySize = 0;
    bool entryExists = false;


    //MessageHdr *msg;
    MessageHdr *joinRepMsg;
    size_t joinRepMsgSize;
    char *clusterMemberInfo;
    Address initiator, tableEntry;
    Address sendTo;
    long *joinreptimestamp;
    vector<MemberListEntry *>::iterator iter;
    char debugMsg[6000];
    GossipMsg *recvdGossipMsg;
    /* int gossipMsgSize;    */

    switch(recvdMsgType) {
        case JOINREQ:
          recvdPort = *(short *)&sender[4]; 
          recvdId = *(int *)&sender[0];
          entrySize =  sizeof(int) + sizeof(short) + sizeof(long) + sizeof(long); 
          initiator = getJoinAddress();  
recvdMember.setid(recvdId);
          recvdMember.setport(recvdPort);
          recvdMember.setheartbeat(*recvdHeartbeat);
          recvdMember.settimestamp(par->getcurrtime());

          /* if entry already exists in the membership table, 
             do not overwrite and do not update the membership table */
          
          memberTableSize = memberNode->memberList.size();
          for (int i = 0;i< memberTableSize; i++) {

             if (memberNode->memberList[i].id == recvdId) {
                entryExists = true;
#ifdef DEBUGLOG
        sprintf(debugMsg, " entry for node %d : already exists. Membership size : %d", recvdId, memberTableSize);
        log->LOG(&memberNode->addr, debugMsg);
#endif 
             }   

          }

          if (entryExists == false) {
             memberNode->memberList.push_back(recvdMember); 
             memberNode->myPos = memberNode->memberList.end();
             memberTableSize = memberNode->memberList.size();         
#ifdef DEBUGLOG
        sprintf(debugMsg, " Inserted entry for node %d : Membership size : %d", recvdId, memberTableSize);
        log->LOG(&memberNode->addr, debugMsg);
#endif             
         }
          
            /* Prepare and send a JOINREP message */
            /* Loop through every entry in the membership table and send the same JOINREP message */
        if (entryExists == false) {
            for (int i = 0; i < memberTableSize; i++ ) {               
              joinRepMsgSize = sizeof(MessageHdr) + entrySize * memberTableSize;              
              joinRepMsg = (MessageHdr *)malloc(joinRepMsgSize * sizeof(char)*2);
              clusterMemberInfo = (char *)(joinRepMsg + 1);
              
              joinRepMsg->msgType = JOINREP; //JOINREP
              for (int j = 0; j < memberTableSize; j++) {
                memcpy(clusterMemberInfo, (char *) &memberNode->memberList[j].id, sizeof(int));
                memcpy(clusterMemberInfo + sizeof(int), (char *) &memberNode->memberList[j].port, sizeof(short));
                memcpy(clusterMemberInfo + sizeof(int) + sizeof(short), (char *) &memberNode->memberList[j].heartbeat, sizeof(long));
                memcpy(clusterMemberInfo + sizeof(int) + sizeof(short) + sizeof(long), 
                            (char *) &memberNode->memberList[j].timestamp, sizeof(long));
                clusterMemberInfo += sizeof(int) + sizeof(short) + sizeof(long) + sizeof(long);

                if ( 0 == memcmp((char *)&(sendTo.addr), (char *)&initiator, sizeof(memberNode->addr.addr))) {
                    tableEntry.init();                
                    *(int *)(&(tableEntry.addr)) = memberNode->memberList[j].id;
                    *(short *)(&(tableEntry.addr[4])) = memberNode->memberList[j].port; 
                    log->logNodeAdd(&initiator, &tableEntry);
                }
                
              }


              
              sendTo.init();
              *(int *)(&(sendTo.addr))= memberNode->memberList[i].getid();
              *(short *)(&(sendTo.addr[4]))=0;
              // Send a JOIN REP message from introducer node back to the initiator node
               initiator = getJoinAddress();


              if ( 0 != memcmp((char *)&(sendTo.addr), (char *)&initiator, sizeof(memberNode->addr.addr))) {
                    
                    /* this initiator node is sending JOINREP to non-initiator node */
                    emulNet->ENsend(&initiator, &sendTo, (char *)joinRepMsg, joinRepMsgSize);
#ifdef DEBUGLOG
        sprintf(debugMsg, " Sending JOINREP to %d : Membership size : %d of size %d", memberNode->memberList[i].getid(), memberTableSize, joinRepMsgSize);
        log->LOG(&memberNode->addr, debugMsg);
#endif 
              }
                             
          }
          /* save the adjusted memberTable Size */
          memberTableSize = memberNode->memberList.size();

        } 


          
          break;  
        case JOINREP:
            entrySize =  sizeof(int) + sizeof(short) + sizeof(long) + sizeof(long);          
            /* reset the membership table, initiator will always send all nodes known at the time of join */
#ifdef DEBUGLOG
            sprintf(debugMsg, " Recvd JOINREP of size %d", size);
            log->LOG(&memberNode->addr, debugMsg);
#endif 
            memberNode->inGroup = true;
            //initMemberListTable(memberNode);
            for ( int i = 4; i < size; i += entrySize) {
                entryExists = false; /* reset entryExists flag for the next entry in the table */
                recvdId = *(int *)(&data[i]);
                recvdMember.setid(recvdId);
                recvdPort = *(short *)(&data[i+sizeof(int)]);
                recvdHeartbeat = (long *)(&data[i+sizeof(int) + sizeof(short)]);
                joinreptimestamp = (long *)(&data[i+sizeof(int) + sizeof(short) + sizeof(long)]);
                recvdMember.setport(recvdPort);
                recvdMember.setheartbeat(*recvdHeartbeat);
                recvdMember.settimestamp(*joinreptimestamp);

                /* before inserting, check if node id already exists in the table */

                for (int k = 0; k <  memberNode->memberList.size(); k++) {
                    if (memberNode->memberList[k].id == recvdId) {
                        entryExists = true;
#ifdef DEBUGLOG
        sprintf(debugMsg, " Duplicate request for node %d", recvdMember.id);
        log->LOG(&memberNode->addr, debugMsg);
#endif 

                    }
                }

                if (entryExists == false) {
                    memberNode->memberList.push_back(recvdMember); 
                    tableEntry.init();                
                    *(int *)(&(tableEntry.addr)) = recvdId;
                    *(short *)(&(tableEntry.addr[4])) = recvdMember.getport();
                    log->logNodeAdd(&self->addr, &tableEntry);
#ifdef DEBUGLOG
        sprintf(debugMsg, " Inserted entry for node %d", recvdMember.id);
        log->LOG(&memberNode->addr, debugMsg);
#endif
                }                                          
           }

            /* save the adjusted memberTable Size */
            memberTableSize = memberNode->memberList.size();
#ifdef DEBUGLOG
        sprintf(debugMsg, " Membership size : %d", memberTableSize);
        log->LOG(&memberNode->addr, debugMsg);
#endif 
          break;  
        case GOSSIP:
            recvdGossipMsg = (GossipMsg *)data;
           
           
            entrySize =  sizeof(int) + sizeof(short) + sizeof(long) + sizeof(long); 
            memberTableSize = memberNode->memberList.size();

            for (int i = 0; i < size - sizeof(MessageHdr); i+= entrySize) {
                
                recvdMember.id = *(int *)(recvdGossipMsg->payload + i);
                recvdMember.port = *(short *)(recvdGossipMsg->payload + i + sizeof(int));
                recvdMember.heartbeat = *(long *)(recvdGossipMsg->payload + i + sizeof(int) + sizeof(short));
                recvdMember.timestamp = *(long *)(recvdGossipMsg->payload + i + sizeof(int) + sizeof(short)+sizeof(long));
                


                
                cout << "\nrecvd (GOSSIP) id , port : " << recvdMember.id << " , " << recvdMember.port 
                                                        << " heartbeat =  "
                                                        << recvdMember.heartbeat << " timestamp = " 
                                                        << recvdMember.timestamp << " i : " << i << endl;
                      
                     

                             /* Merge the info to the local table */
                for (int k = 0; k < memberTableSize; k++) {
                    if (recvdMember.id == self->addr.addr[0]) {
                        
                        cout << "Skip this entry because it points to self. ( " << recvdMember.id 
                             << " ) Node will update its own heartbeat and timestamp " << endl;
                        break;
                             
                    }
                    else if (recvdMember.id == memberNode->memberList[k].id) {
                        /* update the heartbeat and timestamp, if received values are higher */
                        
                        cout << "Update the entry for ( " << recvdMember.id 
                             << " ) by updating heartbeat and timestamp " << endl;    
                             
                        if (recvdMember.heartbeat > memberNode->memberList[k].heartbeat) {
                            memberNode->memberList[k].heartbeat = recvdMember.heartbeat;
                            memberNode->memberList[k].timestamp = par->getcurrtime();
                        }                     

                    }
                }          
            }   

            /* save the adjusted memberTable Size */
            memberTableSize = memberNode->memberList.size();
            cout << "At the end of GOSSIP message processing the table size is ( " << memberTableSize 
                             << " )  " << endl;  
  
           break;  



        case DUMMYLASTMSGTYPE:
          printf("Recieved DUMMY LAST MSG TYPE message\n");
          break; 

        default: 
          printf("Unknown Message\n");
          break;
        }

    return true;
}

/**
 * FUNCTION NAME: nodeLoopOps
 *
 * DESCRIPTION: Check if any node hasn't responded within a timeout period and then delete
 * 				the nodes
 * 				Propagate your membership list
 */
void MP1Node::nodeLoopOps() {

	/*
	 * Your code goes here
	 */

    GossipMsg gossipMsg;
    char *msgdump;
    unsigned memberTableSize;
    size_t gossipMsgSize ;
    /* char * gossipMemberList; */
    Address sendTo;
    int myCurrTime = par->getcurrtime();
    Address tableEntry;
    char debugMsg[5000];

    /* Check that self has not been marked failed */

    if (memberNode->bFailed == true) {
        cout << "Failed Node! Nothing to do." << endl;
        return;
    }

    /* Scan membership table */

#ifdef DEBUGLOG
        sprintf(debugMsg, " Current Timestamp is %d", myCurrTime );
        log->LOG(&memberNode->addr, debugMsg);
#endif     

    memberTableSize = memberNode->memberList.size();
      
    gossipMsgSize =  (sizeof(int) + sizeof(short) + sizeof(long) + sizeof(long)) * memberTableSize; 
    /* protect from core dumps */  
    gossipMsg.payload = (char *)malloc(gossipMsgSize * sizeof(char)*2);

    gossipMsg.msgType = GOSSIP;
    
    memberNode->heartbeat++;
    

    msgdump = (char *)(gossipMsg.payload);

    for (int i = 0, j = 0; i < gossipMsgSize; i+= sizeof(int) + sizeof(short) + sizeof(long) + sizeof(long), j++) {
            
        /* Increment heartbeat and update timestamp, if the entry belongs to self */
        
        if (memberNode->memberList[j].id == memberNode->addr.addr[0]) {
            cout << "\nUpdating heartbeat and timestamp in the membership table" << endl;
            memberNode->memberList[j].timestamp = par->getcurrtime();
            memberNode->memberList[j].heartbeat++; 
            cout << "Done Updating heartbeat and timestamp in the membership table" << endl;
            
        }

       
        memcpy(msgdump + i, &memberNode->memberList[j].id, sizeof(int));
        
        memcpy(msgdump + i + sizeof(int), 
                            &memberNode->memberList[j].port, sizeof(short));
        
        memcpy(msgdump + i + sizeof(int) + sizeof(short), 
                            &memberNode->memberList[j].heartbeat, sizeof(long));

        /* In earlier stages, the timestamp field may not be set */
        if (memberNode->memberList[j].timestamp > 0) {
            
            memcpy(msgdump + i + sizeof(int) + sizeof(short) + sizeof(long), 
                                            &memberNode->memberList[j].timestamp, sizeof(long));
        }
        else {
            /* cout << "Copy timestamp" << endl; */
            memberNode->memberList[j].timestamp = 0;
            memcpy(msgdump + i + sizeof(int) + sizeof(short) + sizeof(long), 
                                            &memberNode->memberList[j].timestamp, sizeof(long));
        }
                
    }



    memberTableSize = memberNode->memberList.size();
    vector<MemberListEntry>::iterator curr = memberNode->memberList.begin();

       
    for (int i = 0;curr != memberNode->memberList.end(); i++) {

        if (myCurrTime > curr->timestamp + GOSSIPTIMEOUT ) {
            /* We have a node down */
            tableEntry.init();                
            *(int *)(&(tableEntry.addr)) = curr->id;
            *(short *)(&(tableEntry.addr[4])) = curr->port;
#ifdef DEBUGLOG
        sprintf(debugMsg, " Remove a node with %d %d %d", *(int *)(&(tableEntry.addr)), myCurrTime, curr->timestamp + GOSSIPTIMEOUT);
        log->LOG(&memberNode->addr, debugMsg);
#endif            
            log->logNodeRemove(&memberNode->addr, &tableEntry);
            curr = memberNode->memberList.erase(curr);

            
            
        } 
        else {

#ifdef DEBUGLOG
        sprintf(debugMsg, " Didn't Remove a node with %d %d %d", curr->id, myCurrTime, curr->timestamp + GOSSIPTIMEOUT);
        log->LOG(&memberNode->addr, debugMsg);
#endif     
            curr++;
        }
     
    }

    memberTableSize = memberNode->memberList.size();
    cout << "Membership List size (after removing faulty nodes) " << memberTableSize << endl;     
    cout << "Current timestamp at " <<  memberNode->addr.getAddress() << " is " << myCurrTime <<endl;
    for (int i = 0; i < memberTableSize; i++) {  
                cout << "timestamp in the Gossip table for " << memberNode->memberList[i].id << " is " 
             << memberNode->memberList[i].timestamp << endl;
    }

     /* loop through the member table and send the gossip table to all nodes except to self */

    for (int i = 0; i < memberTableSize; i++) {   
        
        sendTo.init();
        *(int *)(&(sendTo.addr))= memberNode->memberList[i].getid();
        *(short *)(&(sendTo.addr[4]))=memberNode->memberList[i].getport();

        emulNet->ENsend(&memberNode->addr, &sendTo, (char *)&gossipMsg, gossipMsgSize + sizeof(MessageHdr));

    }
    
    return;
}

/**
 * FUNCTION NAME: isNullAddress
 *
 * DESCRIPTION: Function checks if the address is NULL
 */
int MP1Node::isNullAddress(Address *addr) {
	return (memcmp(addr->addr, NULLADDR, 6) == 0 ? 1 : 0);
}

/**
 * FUNCTION NAME: getJoinAddress
 *
 * DESCRIPTION: Returns the Address of the coordinator
 */
Address MP1Node::getJoinAddress() {
    Address joinaddr;

    memset(&joinaddr, 0, sizeof(Address));
    *(int *)(&joinaddr.addr) = 1;
    *(short *)(&joinaddr.addr[4]) = 0;

    return joinaddr;
}

/**
 * FUNCTION NAME: initMemberListTable
 *
 * DESCRIPTION: Initialize the membership list
 */
void MP1Node::initMemberListTable(Member *memberNode) {
	memberNode->memberList.clear();
}

/**
 * FUNCTION NAME: printAddress
 *
 * DESCRIPTION: Print the Address
 */
void MP1Node::printAddress(Address *addr)
{
    printf("%d.%d.%d.%d:%d \n",  addr->addr[0],addr->addr[1],addr->addr[2],
                                                       addr->addr[3], *(short*)&addr->addr[4]) ;    
}
