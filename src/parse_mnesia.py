# many ideas taken from https://github.com/jeffbryner/rdqdump
#  - reading bytes from files
#  - convert_hex
#  - using find on the hex string to locate messages

import os
import sys
import io
from optparse import OptionParser
import json


# TODO : set allow user to specify the file
vhosts = ['23GGHMUBU0M53JQSJA0D1RLN4','52EPDX2M6A2FXCJA589KMI773','DYMMNWGOXAPNGDT65THSYNKDH','2KVL28AS0JEHD0UYQNDTX3QIO','628WB79CIFDYO9LJI6DKMI09L']
host_id = 0
root_dir = '/usr/local/var/lib/rabbitmq/mnesia/rabbit/msg_stores/vhosts/'
rqdfile = '/msg_store_persistent/0.rdq'
fileName = root_dir + vhosts[host_id] + '/msg_store_persistent/0.rdq'


fileName='/usr/local/var/lib/rabbitmq/mnesia/rabbit/msg_stores/vhosts/2KVL28AS0JEHD0UYQNDTX3QIO/queues/7NJWTXU5MSR21TJGJEOA2NPC2/0.idx'
print(fileName)


def readChunk(data,start,end):
    data.seek(int(start))
    readdata=data.read(end)
    return readdata


def convert_hex(string):
    return ''.join([hex(character)[2:].upper().zfill(2) \
                     for character in string])


def extractString(data, hexString, byteLengthHex):
	hexdata=convert_hex(data)
	dataPos=hexdata.find(hexString.upper())
	if dataPos == -1:
		found=False
		unescapedData=''
		endPosition = -1
	else:
		found=True
		startofMatch=round(dataPos/2)
		recordSize = data[startofMatch+byteLengthHex:startofMatch+byteLengthHex+2]
		rdqEntryLength = int(convert_hex(recordSize), 16)
		dataMessage = data[startofMatch+byteLengthHex+2: startofMatch+byteLengthHex+2+rdqEntryLength]
		unescapedData = dataMessage.decode('ascii', 'ignore')
		endPosition =  startofMatch+byteLengthHex+2+rdqEntryLength
	return {"string": unescapedData, "endpos": endPosition, 'found':found}

def grabMessages(data, messageHex, exchangeHex, initial_type):
	cutData=data
	content_type = initial_type
	byteLengthX = round(len(exchangeHex)/2)
	byteLengthM = round(len(messageHex)/2)
	moreBytes=True
	exchange_list=[]
	message_list=[]
	while moreBytes:
		if content_type == 'exchange':
			resp = extractString(cutData, exchangeHex, byteLengthX)
			if resp['found']:
				endPosition = resp['endpos']
				exchange=resp['string']
				exchange_list.append(exchange)
				cutData=cutData[endPosition:]
				content_type = 'message'
			else:
				moreBytes=False
		else:
			resp = extractString(cutData, messageHex, byteLengthM)
			if resp['found']:
				endPosition = resp['endpos']
				message=resp['string']
				message_list.append(message)
				cutData=cutData[endPosition:]
				content_type = 'exchange'
			else:
				moreBytes=False
	return {'messages': message_list, 'exchanges': exchange_list, 'remaining_data': cutData, 'current_type': content_type}


msgHex = "395f316c000000016d0000"
exchangeHex = '000000006C000000016D0000'

byteChunk = 2**16
src=open(fileName,'rb')
startByte=0
fileByteCount = os.path.getsize(fileName)
byteChunk=min(fileByteCount,byteChunk)

moreBytes = True
messages = []
exchanges=[]
data = readChunk(src,src.tell(),byteChunk)
ctr=0
while moreBytes:
	
	initial_type = 'exchange'
	resp = grabMessages(data, msgHex, exchangeHex, initial_type)
	oldData = resp['remaining_data']
	messages+= resp['messages']
	exchanges += resp['exchanges']
	initial_type = resp['current_type']
	if src.tell() >= byteChunk:
		moreBytes = False
		src.close()
	else:
		ctr+=1
		if ctr > 40:
			moreBytes=False
		dataChunk = readChunk(src,src.tell(),byteChunk)
		print(src.tell())
		data = oldData + dataChunk
	

# TODO: output data to json file or other
print(messages)
print(exchanges)


