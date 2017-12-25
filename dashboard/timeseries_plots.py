import numpy as np
from datetime import datetime
from bokeh.models import ColumnDataSource, HoverTool, SaveTool
from bokeh.plotting import figure


class TimeSeriesPlot(object):
	tools = 'pan, xwheel_zoom, xbox_zoom, save, reset'
	hover = HoverTool(tooltips=[("Time", "@display_time"), ("Data", "@data")])
	x_range = None

	def __init__(self, request_metrics, plot_metrics, line_colors):
		self.request_metrics = request_metrics
		self.plot_metrics = plot_metrics
		self.line_colors = line_colors
		self.data_sources = {}

	def create_plot(self, title, width, height, xaxis_label='Time', yaxis_label='Data', line_width=3):
		if TimeSeriesPlot.x_range is None:
			p = figure(plot_width=width, plot_height=height, x_axis_type='datetime', tools=self.tools, 
					   title=title)
			TimeSeriesPlot.x_range = p.x_range
		else:
			p = figure(plot_width=width, plot_height=height, x_axis_type='datetime', tools=self.tools, 
					   title=title, x_range=TimeSeriesPlot.x_range)
		for metric in self.plot_metrics:
			self.data_sources[metric] = ColumnDataSource(dict(time=[], display_time=[], data=[]))
			p.line(source=self.data_sources[metric], x='time', y='data', line_color=self.line_colors[metric],
				   line_width=3, legend=metric)
		p.y_range.start = 0
		p.xaxis.axis_label = xaxis_label
		p.yaxis.axis_label = yaxis_label
		p.add_tools(self.hover)
		self.p = p
		return p

	def update_plot(self, time, display_time, updated_data):
		for metric in self.plot_metrics:
			if updated_data[metric] is not None:
				data = [float(updated_data[metric])]
				self.data_sources[metric].stream(dict(time=time, display_time=display_time, data=data))


class BytesPlot(TimeSeriesPlot):
	request_metrics = ['bytesout', 'bytesin']
	plot_metrics = ['bytesout', 'bytesin']
	line_colors = {'bytesout': 'dodgerblue', 'bytesin': 'mediumseagreen'}

	def __init__(self):
		super(BytesPlot, self).__init__(self.request_metrics, self.plot_metrics, self.line_colors)

	def create_plot(self):
		return super(BytesPlot, self).create_plot('BytesOut/BytesIn', 600, 250, 
												  yaxis_label='Data Rate [Kbytes/sec]')

	def update_plot(self, time, display_time, updated_data):
		for metric in self.plot_metrics:
			if updated_data[metric] is not None:
				data = [float(updated_data[metric]) / 1024] # bytes/s -> Kbytes/s
				self.data_sources[metric].stream(dict(time=time, display_time=display_time, data=data))


class LagsPlot(TimeSeriesPlot):
	request_metrics = ['offsets']
	plot_metrics = ['max', 'min', 'mean']
	line_colors = {'max': 'lightcoral', 'min': 'plum', 'mean': 'palegoldenrod'}

	def __init__(self):
		super(LagsPlot, self).__init__(self.request_metrics, self.plot_metrics, self.line_colors)

	def create_plot(self):
		return super(LagsPlot, self).create_plot('Offset Lags', 600, 250, yaxis_label='Offset Lags')

	def update_plot(self, time, display_time, updated_data):
		if updated_data['offsets'] is None:
			return
		partitions = updated_data['offsets']
		lags = [partitions[p]['lag'] for p in partitions]
		data = {'max': max(lags), 'min': min(lags), 'mean': np.mean(lags)}
		for metric in self.plot_metrics:
			d = [data[metric]]
			self.data_sources[metric].stream(dict(time=time, display_time=display_time, data=d))


class MsgsizePlot(TimeSeriesPlot):
	request_metrics = ['msgsize']
	plot_metrics = ['msgsize']
	line_colors = {'msgsize': 'skyblue'}

	def __init__(self):
		super(MsgsizePlot, self).__init__(self.request_metrics, self.plot_metrics, self.line_colors)

	def create_plot(self):
		return super(MsgsizePlot, self).create_plot('Average Message Size', 600, 250, 
													yaxis_label='Average Message Size [bytes]')
