import json
import random
import socket
from datetime import datetime
from bokeh.layouts import column, layout
from bokeh.plotting import curdoc, figure
from timeseries_plots import MsgsPlot, BytesPlot, VmPlot, LagsPlot, MsgsizePlot, WaitTimePlot

METRICS_SERVER_IP = 'localhost'
METRICS_SERVER_PORT = 9999
BUFFER_SIZE = 1024
UPDATE_INTERVAL_MSEC = 3000
PLOTS = [[MsgsPlot(), VmPlot()], [LagsPlot(), MsgsizePlot()], [WaitTimePlot()]]

def connect(ip, port):
	sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	sock.connect((ip, port))
	return sock

def flatten_list(list):
	flat_list = []
	for sublist in list:
		for item in sublist:
			if item is not None:
				flat_list.append(item)
	return flat_list

def get_requests(plots):
	flat_plots = flatten_list(plots)
	requests = flatten_list([plot.get_requests() for plot in flat_plots])
	return list(set(requests))

def update_plots():
	now = datetime.now()
	time = [now]
	display_time = [now.strftime("%m-%d-%Y %H:%M:%S.%f")]

	args = {'args': requests}
	req = 'get ' + json.dumps(args)
	sock.send(req)
	resp = sock.recv(BUFFER_SIZE)
	resp = resp[-1] if resp.endswith('\n') else resp

	display_data = []
	if resp.startswith('ok'):
		data = json.loads(resp[3:])
		for plot in flat_plots:
			d = plot.update_plot(time, display_time, data)
			display_data = display_data + d

	# print ', '.join(str(x) for x in display_data)
	print resp


print('\tConnecting to: {}:{}'.format(METRICS_SERVER_IP, METRICS_SERVER_PORT))
sock = connect(METRICS_SERVER_IP, METRICS_SERVER_PORT)

requests = get_requests(PLOTS)
print('\tMaking requests to: {}'.format(requests))
flat_plots = flatten_list(PLOTS)

l = layout([map(lambda p: p.create_plot(), plot) for plot in PLOTS])
curdoc().add_root(l)
curdoc().add_periodic_callback(update_plots, UPDATE_INTERVAL_MSEC)
curdoc().title = "Kafka Metrics Visualizer"
