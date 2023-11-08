# many ideas taken from https://github.com/jeffbryner/rdqdump
#  - reading bytes from files
#  - convert_hex
#  - using find on the hex string to locate messages

import os
import sys
import io
import argparse
import json, time

t0 = time.time()

parser = argparse.ArgumentParser()
parser.add_argument('-file', help='choose file to parse', default='no file')
parser.add_argument('-chunk', help='byte count for each file chunk to read (default=1028)', default=1028)
parser.add_argument('-o', help='filename (no extension) for file; print for print to terminal', default='print')
parser.add_argument('-be', help='byte endianess for lenght, e.g. big, little', default="big")
parser.add_argument("-e", help="encoding, e.g. utf-8, ascii", default="utf-8")
parser.add_argument("-ee", help="encoding errors, e.g. strict, ignore", default="strict")
args = parser.parse_args()

if args.file == 'no file':
	print('[ERROR] please specify a file name to parse using "-file filename"')
	sys.exit()
#print(file)
#print(args['file'])

# TODO : set allow user to specify the file
vhosts = ['23GGHMUBU0M53JQSJA0D1RLN4','52EPDX2M6A2FXCJA589KMI773','DYMMNWGOXAPNGDT65THSYNKDH','2KVL28AS0JEHD0UYQNDTX3QIO','628WB79CIFDYO9LJI6DKMI09L']
host_id = 0
root_dir = '/usr/local/var/lib/rabbitmq/mnesia/rabbit/msg_stores/vhosts/'
rqdfile = '/msg_store_persistent/0.rdq'
fileName = root_dir + vhosts[host_id] + '/msg_store_persistent/0.rdq'

fileName = args.file

endianness = args.be
encoding = args.e
encoding_errors = args.ee

#fileName='/usr/local/var/lib/rabbitmq/mnesia/rabbit/msg_stores/vhosts/2KVL28AS0JEHD0UYQNDTX3QIO/queues/7NJWTXU5MSR21TJGJEOA2NPC2/0.idx'
print('Reading file: ' + fileName)


def readChunk(data,start,end):
    data.seek(int(start))
    readdata=data.read(end)
    return readdata


def convert_hex(string):
    return ''.join([hex(character)[2:].upper().zfill(2) \
                     for character in string])


def extractString(data, hexString, byteLengthHex):
	dataPos = data.find(bytes.fromhex(hexString))
	if dataPos == -1:
		found=False
		unescapedData=''
		endPosition = -1
	else:
		found=True
		startofMatch=round(dataPos)
		recordSize = data[startofMatch+byteLengthHex:startofMatch+byteLengthHex+2]
		rdqEntryLength = int.from_bytes(recordSize, endianness)
		dataMessage = data[startofMatch+byteLengthHex+2: startofMatch+byteLengthHex+2+rdqEntryLength]
		unescapedData = dataMessage.decode(encoding, encoding_errors)
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

# hex string that indicates message 
# Matching binary might be faster
msgBin = b'9_1l\x00\x00\x00\x01m\x00\x00'
msgHex = "395f316c000000016d0000"
# hex string that indicates exchange name
exchangeHex = '000000006C000000016D0000'
xchBin = b'\x00\x00\x00\x00l\x00\x00\x00\x01m\x00\x00'


byteChunk = 2**19
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
	if src.tell() >= fileByteCount:
		moreBytes = False
		src.close()
	else:
		ctr+=1
		#if ctr > 40:
		#	moreBytes=False
		dataChunk = readChunk(src,src.tell(),byteChunk)
		print(round(src.tell()/fileByteCount,3))
		data = oldData + dataChunk
	

print("parse time: {0}".format(time.time()-t0))
print("Number of chunks read: {0}".format(ctr))
print("Number of messages: {0}".format(len(messages)))

if args.o != 'print':
	# output data to json file or other
	output = tuple(zip(exchanges, messages)) 
	outFile = args.o + '.json'
	with open(outFile, 'w', encoding='utf-8') as f:
		json.dump(output, f, ensure_ascii=False, indent=4)
	print('Data saved as: ' + outFile)
else :
	print(messages)
	print(exchanges)


