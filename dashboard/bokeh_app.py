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
REQUEST_METRICS = ['msgsin', 'msgsout', 'lags', 'jmx']
# REQUEST_METRICS = ['jmx']
# REQUEST_METRICS = ['bytesout', 'bytesin', 'offsets', 'bytesout_1minavg', 'bytesin_1minavg', 'msgsin_1minavg', 'vm']
PLOTS = [MsgsPlot(), LagsPlot(), MsgsizePlot()]
# PLOTS = [MsgsizePlot()]


def connect(ip, port):
	sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	sock.connect((ip, port))
	return sock
					

def update_plots():
	now = datetime.now()
	time = [now]
	display_time = [now.strftime("%m-%d-%Y %H:%M:%S.%f")]

	args = {'args': REQUEST_METRICS}
	req = 'get ' + json.dumps(args)
	sock.send(req)
	resp = sock.recv(BUFFER_SIZE)
	print resp
	resp = resp[-1] if resp.endswith('\n') else resp

	if resp.startswith('ok'):
		data = json.loads(resp[3:])
		for plot in PLOTS:
			plot.update_plot(time, display_time, data)

sock = connect(METRICS_SERVER_IP, METRICS_SERVER_PORT)
column_plots = [plot.create_plot() for plot in PLOTS]

curdoc().add_root(column(column_plots))
curdoc().add_periodic_callback(update_plots, UPDATE_INTERVAL_MSEC)
curdoc().title = "Kafka Metrics Visualizer"
