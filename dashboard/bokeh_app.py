import random
import socket
from datetime import datetime
# from bokeh.layouts import row, column, gridplot
from bokeh.models import ColumnDataSource, HoverTool, SaveTool
from bokeh.plotting import curdoc, figure

DATA_SERVER_IP = 'localhost'
DATA_SERVER_PORT = 9999
BUFFER_SIZE = 1024
TARGET_DATA = ['bytesout', 'bytesin']
LINE_COLORS = {'bytesout':'blue', 'bytesin':'green'}
UPDATE_INTERVAL_MSEC = 3000

data_sources = {}

plt = figure(plot_width=800,
			 plot_height=400,
			 x_axis_type='datetime',
			 tools="xpan, xwheel_zoom, xbox_zoom, save, reset",
			 title="Kafka BytesOut/BytesIn")
for entry in TARGET_DATA:
	data_sources[entry] = ColumnDataSource(dict(time=[], display_time=[], data=[]))
	plt.line(source=data_sources[entry], x='time', y='data', line_color=LINE_COLORS[entry], line_width=3, legend=entry)
plt.y_range.start = 0
plt.xaxis.axis_label = "Time"
plt.yaxis.axis_label = "Data Rate [Kbytes/sec]"
hover = HoverTool(tooltips=[
    ("Time", "@display_time"),
    ("Data", "@data")])
plt.add_tools(hover)

def connect(ip, port):
	sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	sock.connect((ip, port))
	return sock

def update_data():
	now = datetime.now()
	time = [now]
	display_time = [now.strftime("%m-%d-%Y %H:%M:%S.%f")]

	# TODO: check if sock is open
	req = 'get ' + ','.join(TARGET_DATA) + '\n'
	sock.send(req)
	resp = sock.recv(BUFFER_SIZE)
	resp = resp[-1] if resp.endswith('\n') else resp

	if resp == 'None':
		print 'No data returned to request: {}'.format(req)
	else:
		vals = resp.split(',')
		for val in vals:
			items = val.split(':')
			entry = items[0]
			data = [float(items[1]) / 1024] # bytes/s -> Kbytes/s
			data_sources[entry].stream(dict(time=time, display_time=display_time, data=data))
		print("{}: {}".format(display_time, resp))


sock = connect(DATA_SERVER_IP, DATA_SERVER_PORT)
curdoc().add_root(plt)
curdoc().add_periodic_callback(update_data, UPDATE_INTERVAL_MSEC)
curdoc().title = "Kafka Metrics Visualizer"
