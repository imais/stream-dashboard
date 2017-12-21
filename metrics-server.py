import socket 
import threading
from threading import Thread, Lock 

# Constants
TCP_IP = '0.0.0.0' 
TCP_PORT = 9999
BUFFER_SIZE = 256

# Global variables
val_store = {}
store_lock = Lock()
threads = [] 

class ClientThread(Thread): 
	def __init__(self, ip, port, conn): 
		Thread.__init__(self) 
		self.ip = ip 
		self.port = port 
		self.conn = conn

	def dbg_print(self, msg):
		print '{}: {}'.format(threading.current_thread().name, msg)

	def set_val(self, var, val):
		# print 'Acquiring lock'
		store_lock.acquire()
		try:
			val_store[var] = val
		finally:
			# print 'Releasing lock'
			store_lock.release()

	def get_val(self, vars):
		try:
			# get a,b --> a:100,b:100
			vals = [var + ":" + val_store[var] for var in vars]
		except:
			self.dbg_print('One of the values not found for: {}'.format(vars))
			vals = 'None'
		finally:
			return vals
		
	def run(self): 
		self.dbg_print('[+] New server socket thread started for {}:{}'.format(ip, port))

		while True : 
			self.dbg_print('Server blocked on recv')
			req = self.conn.recv(BUFFER_SIZE)
			req = req[:-1] if req.endswith('\n') else req
			req = req[:-1] if req.endswith('\r') else req
			self.dbg_print('Server received {} bytes: {}'.format(len(req), req))

			# handing requests
			if len(req) == 0 or req == 'bye' or req == 'quit':
				self.conn.close()
				break
			elif req.startswith('set'):
				# format: "set var val"
				items = req.split(' ')
				if len(items) != 3:
					self.dbg_print('Illegal set command: {}'.format(req))
				else:
					self.set_val(items[1], items[2])
			elif req.startswith('get'):
				# format: "get var1,var2,...," note: no space between csv
				items = req.split(' ')
				if len(items) != 2:
					self.dbg_print('Illegal get command: {}'.format(req))
				else:
					vals = self.get_val(items[1].split(','))
					print vals
					resp = 'None' if vals == 'None' else ','.join(vals)
					self.conn.send(resp)
			else:
				self.dbg_print('Received non-supported command: {}'.format(req))
				
		self.dbg_print('[-] Terminating thread for {}:{}'.format(self.ip, self.port))

server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM) 
server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1) 
server_sock.bind((TCP_IP, TCP_PORT)) 

while True: 
	server_sock.listen(4)		# number of backlogged clients
	print "Data server listening on port {}".format(TCP_PORT)
	(conn, (ip, port)) = server_sock.accept() 
	thread = ClientThread(ip, port, conn) 
	thread.start() 
	threads.append(thread)
	
for t in threads: 
	t.join() 
