#!/bin/bash

#################################################
# FILE NAME: KVStoreGrader.sh
#
# DESCRIPTION: Grader for MP2
#
# RUN PROCEDURE:
# $ chmod +x KVStoreGrader.sh
# $ ./KVStoreGrader.sh
#################################################

function contains () {
    local e
    for e in "${@:2}"
  	do
    	if [ "$e" == "$1" ]; then
      		echo 1
      		return 1;
    	fi
  	done
    echo 0
}

####
# Main function
####

verbose=$(contains "-v" "$@")

###
# Global variables
###
SUCCESS=0
FAILURE=-1
RF=3
RFPLUSONE=4
CREATE_OPERATION="CREATE OPERATION"
CREATE_SUCCESS="create success"
GRADE=0
DELETE_OPERATION="DELETE OPERATION"
DELETE_SUCCESS="delete success"
DELETE_FAILURE="delete fail"
INVALID_KEY="invalidKey"
READ_OPERATION="READ OPERATION"
READ_SUCCESS="read success"
READ_FAILURE="read fail"
QUORUM=2
QUORUMPLUSONE=3
UPDATE_OPERATION="UPDATE OPERATION"
UPDATE_SUCCESS="update success"
UPDATE_FAILURE="update fail"

echo ""
echo "############################"
echo " CREATE TEST"
echo "############################"
echo ""

CREATE_TEST_STATUS="${SUCCESS}"
CREATE_TEST_SCORE=0

if [ "${verbose}" -eq 0 ]
then
    make clean > /dev/null 2>&1
    make > /dev/null 2>&1
    if [ $? -ne "${SUCCESS}" ]
    then
    	echo "COMPILATION ERROR !!!"
    	exit
    fi
    ./Application ./testcases/create.conf > /dev/null 2>&1
else
	make clean
	make
	if [ $? -ne "${SUCCESS}" ]
	then
    	echo "COMPILATION ERROR !!!"
    	exit
    fi
	./Application ./testcases/create.conf
fi

echo "TEST 1: Create 3 replicas of every key"

create_count=`grep -i "${CREATE_OPERATION}" dbg.log | wc -l`
create_success_count=`grep -i "${CREATE_SUCCESS}" dbg.log | wc -l`
expected_count=$(( ${create_count} * ${RFPLUSONE} ))

if [ ${create_success_count} -ne ${expected_count} ]
then 
	CREATE_TEST_STATUS="${FAILURE}"
else
	keys=`grep -i "${CREATE_OPERATION}" dbg.log | cut -d" " -f7`
	for key in ${keys}
	do 
		key_create_success_count=`grep -i "${CREATE_SUCCESS}" dbg.log | grep "${key}" | wc -l`
		if [ "${key_create_success_count}" -ne "${RFPLUSONE}" ]
		then
			CREATE_TEST_STATUS="${FAILURE}"
			break
		fi
	done
fi

if [ "${CREATE_TEST_STATUS}" -eq "${SUCCESS}" ] 
then
	CREATE_TEST_SCORE=3	
fi

# Display score
echo "TEST 1 SCORE..................: ${CREATE_TEST_SCORE} / 3"
# Add to grade
GRADE=$(( ${GRADE} + ${CREATE_TEST_SCORE} ))

#echo ""
#echo "############################"
#echo " CREATE TEST ENDS"
#echo "############################"
#echo ""

echo ""
echo "############################"
echo " DELETE TEST"
echo "############################"
echo ""

DELETE_TEST1_STATUS="${SUCCESS}"
DELETE_TEST2_STATUS="${SUCCESS}"
DELETE_TEST1_SCORE=0
DELETE_TEST2_SCORE=0

if [ "${verbose}" -eq 0 ]
then
    make clean > /dev/null 2>&1
    make > /dev/null 2>&1
    if [ $? -ne "${SUCCESS}" ]
    then
    	echo "COMPILATION ERROR !!!"
    	exit
    fi
    ./Application ./testcases/delete.conf > /dev/null 2>&1
else
	make clean
	make
	if [ $? -ne "${SUCCESS}" ]
	then
    	echo "COMPILATION ERROR !!!"
    	exit
    fi
	./Application ./testcases/delete.conf
fi

echo "TEST 1: Delete 3 replicas of every key"

delete_count=`grep -i "${DELETE_OPERATION}" dbg.log | wc -l`
valid_delete_count=$(( ${delete_count} - 1 ))
expected_count=$(( ${valid_delete_count} * ${RFPLUSONE} ))
delete_success_count=`grep -i "${DELETE_SUCCESS}" dbg.log | wc -l`

if [ "${delete_success_count}" -ne "${expected_count}" ]
then
	DELETE_TEST1_STATUS="${FAILURE}"
else 
	keys=""
	keys=`grep -i "${DELETE_OPERATION}" dbg.log | cut -d" " -f7`
	for key in ${keys}
	do 
		if [ $key != "${INVALID_KEY}" ]
		then
			key_delete_success_count=`grep -i "${DELETE_SUCCESS}" dbg.log | grep "${key}" | wc -l`
			if [ "${key_delete_success_count}" -ne "${RFPLUSONE}" ]
			then
				DELETE_TEST1_STATUS="${FAILURE}"
				break
			fi
		fi
	done
fi

echo "TEST 2: Attempt delete of an invalid key"

delete_fail_count=`grep -i "${DELETE_FAILURE}" dbg.log | grep "${INVALID_KEY}" | wc -l`
if [ "${delete_fail_count}" -ne 4 ]
then
	DELETE_TEST2_STATUS="${FAILURE}"
fi

if [ "${DELETE_TEST1_STATUS}" -eq "${SUCCESS}" ]
then
	DELETE_TEST1_SCORE=3
fi

if [ "${DELETE_TEST2_STATUS}" -eq "${SUCCESS}" ]
then
	DELETE_TEST2_SCORE=4
fi

# Display score
echo "TEST 1 SCORE..................: ${DELETE_TEST1_SCORE} / 3"
echo "TEST 2 SCORE..................: ${DELETE_TEST2_SCORE} / 4"
# Add to grade
GRADE=$(( ${GRADE} + ${DELETE_TEST1_SCORE} ))
GRADE=$(( ${GRADE} + ${DELETE_TEST2_SCORE} ))

#echo ""
#echo "############################"
#echo " DELETE TEST ENDS"
#echo "############################"
#echo ""

