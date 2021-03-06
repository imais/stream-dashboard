import json
import signal
import socket 
import sys
import threading
from threading import Thread, Lock 
from derived_values import MsgsIn, MsgsOut, OffsetLags, WaitTime

# Constants
TCP_IP = '0.0.0.0' 
TCP_PORT = 9999
BUFFER_SIZE = 8192
LOG_FILE = './log.txt'

# Global variables
val_store = {}
store_lock = Lock()
log_lock = Lock()
threads = [] 
derived_values = {'offsets': [MsgsIn(), MsgsOut(), OffsetLags(), WaitTime()]}

def signal_handler(signal, frame):
	print('\nCaught Ctrl-C signal!!')
	if not f.closed:
		print('Closing {}...'.format(LOG_FILE))
		f.close()
	sys.exit(0)

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
		vals = {}
		for var in vars:
			vals[var] = val_store[var] if var in val_store else None
		return vals

	def write_log(self, data):
		log_lock.acquire()
		try:
			f.write(data)
		finally:
			log_lock.release()

	def run(self): 
		self.dbg_print('[+] New server socket thread started for {}:{}'.format(ip, port))

		while True: 
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
				# format: set {'args': {var1: val1, var2: val2, ...}}
				self.write_log(req + '\n')
				try:
					json_data = json.loads(req[4:])
					args = json_data['args']
					for var in args:
						new_vars = []
						if derived_values.has_key(var):
							for derived_value in derived_values[var]:
								(new_var, new_val) = derived_value.compute(args[var])
								self.set_val(new_var, new_val)
								self.dbg_print('Derived: {}: {}'.format(new_var, new_val))
								new_vars = new_vars + [new_var]
						if var not in new_vars:
							self.set_val(var, args[var])
					resp = 'ok'
				except Exception as ex:
					self.dbg_print('{}, received command: {}'.format(ex, req))
					resp = 'error'
				self.dbg_print('Response: ' + resp)
				self.conn.send(resp)
			elif req.startswith('get'):
				# format: get {'args': [var1, var2, ...]}
				try:
					json_data = json.loads(req[4:])
					vars = json_data['args']
					vals = self.get_val(vars)
					resp = 'error' if vals == 'None' else 'ok ' + json.dumps(vals)
				except Exception as ex:
					self.dbg_print('{}, received command: {}'.format(ex, req))
				self.dbg_print('Response: ' + resp)
				self.conn.send(resp)
			else:
				self.dbg_print('Received non-supported command: {}'.format(req))
				
		self.dbg_print('[-] Terminating thread for {}:{}'.format(self.ip, self.port))

signal.signal(signal.SIGINT, signal_handler)

print('Opening {}'.format(LOG_FILE))
f = open(LOG_FILE, 'w')

server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM) 
server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1) 
server_sock.bind((TCP_IP, TCP_PORT)) 

while True: 
	server_sock.listen(4)		# number of backlogged clients
	print('Data server listening on port {}'.format(TCP_PORT))
	(conn, (ip, port)) = server_sock.accept() 
	thread = ClientThread(ip, port, conn) 
	thread.start() 
	threads.append(thread)
	
for t in threads: 
	t.join() 
