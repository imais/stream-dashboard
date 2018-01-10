import json
import random
import socket
from datetime import datetime
from bokeh.layouts import column
from bokeh.plotting import curdoc, figure
from timeseries_plots import MsgsPlot, BytesPlot, VmPlot, LagsPlot, MsgsizePlot

METRICS_SERVER_IP = 'localhost'
METRICS_SERVER_PORT = 9999
BUFFER_SIZE = 1024
UPDATE_INTERVAL_MSEC = 3000
PLOTS = [BytesPlot(), MsgsPlot(), LagsPlot(), VmPlot(), MsgsizePlot()]


def connect(ip, port):
	sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	sock.connect((ip, port))
	return sock


def get_requests(plots):
	requests = []
	for plot in plots:
		requests = requests + plot.get_requests()
	return list(set(requests)) # unique list


def update_plots():
	now = datetime.now()
	time = [now]
	display_time = [now.strftime("%m-%d-%Y %H:%M:%S.%f")]

	args = {'args': REQUESTS}
	req = 'get ' + json.dumps(args)
	sock.send(req)
	resp = sock.recv(BUFFER_SIZE)
	print resp
	resp = resp[-1] if resp.endswith('\n') else resp

	if resp.startswith('ok'):
		data = json.loads(resp[3:])
		for plot in PLOTS:
			plot.update_plot(time, display_time, data)

print('\tConnecting to: {}:{}'.format(METRICS_SERVER_IP, METRICS_SERVER_PORT))
sock = connect(METRICS_SERVER_IP, METRICS_SERVER_PORT)
column_plots = [plot.create_plot() for plot in PLOTS]

REQUESTS = get_requests(PLOTS)
print('\tMaking requests to: {}'.format(REQUESTS))

curdoc().add_root(column(column_plots))
curdoc().add_periodic_callback(update_plots, UPDATE_INTERVAL_MSEC)
curdoc().title = "Kafka Metrics Visualizer"
