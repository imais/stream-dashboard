import numpy as np
from datetime import datetime
from bokeh.models import ColumnDataSource, HoverTool, Legend, SaveTool
from bokeh.plotting import figure


class TimeSeriesPlot(object):
	tools = 'pan, xbox_zoom, save, reset'
	hover = HoverTool(tooltips=[("Time", "@display_time"), ("Data", "@data")])
	x_range = None
	default_width  = 650
	default_height = 180
	default_line_width = 2
	default_muted_alpha = 0.2

	def __init__(self, metrics, queries, requests, line_colors, line_dashes=None):
		self.metrics = metrics
		self.queries = queries
		self.requests = requests
		self.line_colors = line_colors
		self.line_dashes = line_dashes
		self.data_sources = {}

	def create_plot(self, title, width=default_width, height=default_height, 
					xaxis_label='Time', yaxis_label='Data', line_width=default_line_width):
		if TimeSeriesPlot.x_range is None:
			p = figure(plot_width=width, plot_height=height, x_axis_type='datetime', tools=self.tools, 
					   toolbar_location='above', title=title)
			TimeSeriesPlot.x_range = p.x_range
		else:
			p = figure(plot_width=width, plot_height=height, x_axis_type='datetime', tools=self.tools, 
					   toolbar_location='above', title=title, x_range=TimeSeriesPlot.x_range)

		legend_items = []
		for metric in self.metrics:
			self.data_sources[metric] = ColumnDataSource(dict(time=[], display_time=[], data=[]))
			if self.line_dashes is not None:
				r = p.line(source=self.data_sources[metric], x='time', y='data', color=self.line_colors[metric], line_dash=self.line_dashes[metric], line_width=self.default_line_width, muted_color=self.line_colors[metric], muted_alpha=self.default_muted_alpha)
			else:
				r = p.line(source=self.data_sources[metric], x='time', y='data', color=self.line_colors[metric], line_width=self.default_line_width, muted_color=self.line_colors[metric], muted_alpha=self.default_muted_alpha)
			if 1 < len(self.metrics):
				legend_items.append((metric, [r]))
		legend = Legend(items=legend_items, click_policy='mute')
			
		p.y_range.start = 0
		p.xaxis.axis_label = xaxis_label
		p.yaxis.axis_label = yaxis_label
		p.add_layout(legend, 'right')
		p.add_tools(self.hover)

		self.p = p
		return p

	def update_plot_(self, time, display_time, updated_data):
		for metric in self.metrics:
			if metric in updated_data and updated_data[metric] is not None:
				data = [float(updated_data[metric])]
				self.data_sources[metric].stream(dict(time=time, display_time=display_time, data=data))

	def get_data(self, data, query):
		keys = query.split('#')
		# print('\tquery={}, keys={}'.format(query, keys))
		for key in keys:
			if key not in data:
				return None
			# print('\tkey={}, data[key]={}'.format(key, data[key]))
			if type(data[key]) != dict:
				break
			else:
				data = data[key]
		return data[key]

	def get_requests(self):
		return self.requests

	def update_plot(self, time, display_time, updated_data):
		data = {}
		for metric in self.metrics:
			val = self.get_data(updated_data, self.queries[metric])
			if val is not None:
				self.data_sources[metric].stream(dict(time=time, display_time=display_time, data=[val]))


class MsgsPlot(TimeSeriesPlot):
	metrics = ['msgsin', 'msgsout', 'msgsin_1min', 'msgsout_1min']
	queries = {'msgsin': 'msgsin#msgsin', 'msgsin_1min': 'msgsin#msgsin_1min', 
			   'msgsout': 'msgsout#msgsout', 'msgsout_1min': 'msgsout#msgsout_1min'}
	requests = ['msgsin', 'msgsout']
	line_colors = {'msgsin': 'mediumaquamarine', 'msgsin_1min': 'mediumseagreen',
				   'msgsout': 'lightskyblue', 'msgsout_1min': 'dodgerblue', 'delta': 'palevioletred'}
	line_dashes = {'msgsin': 'solid', 'msgsin_1min': 'dashed',
				   'msgsout': 'solid', 'msgsout_1min': 'dashed', 'delta': 'solid'}

	def __init__(self):
		super(MsgsPlot, self).__init__(self.metrics, self.queries, self.requests,
									   self.line_colors, self.line_dashes)

	def create_plot(self):
		return super(MsgsPlot, self).create_plot('MessagesIn/MessagesOut',
												  yaxis_label='Messages per Second')


class BytesPlot(TimeSeriesPlot):
	metrics = ['bytesout', 'bytesin', 'bytesout_1min', 'bytesin_1min']
	queries = {'bytesout': 'bytesout', 'bytesin': 'bytesin', \
			   'bytesout_1min': 'jmx#bytesout_1min', 'bytesin_1min': 'jmx#bytesin_1min'}
	requests = ['bytesout', 'bytesin', 'jmx']
	line_colors = {'bytesout': 'dodgerblue', 'bytesin': 'mediumseagreen', 'bytesout_1min': 'lightskyblue', 'bytesin_1min': 'mediumaquamarine'}
	line_dashes = {'bytesout': 'solid', 'bytesin': 'solid', 'bytesout_1min': 'dashed', 'bytesin_1min': 'dashed'}

	def __init__(self):
		super(BytesPlot, self).__init__(self.metrics, self.queries, self.requests,
										self.line_colors, self.line_dashes)

	def create_plot(self):
		return super(BytesPlot, self).create_plot('BytesIn/BytesOut',
												  yaxis_label='Data Rate [Kbytes/sec]')

	def update_plot(self, time, display_time, updated_data):
		data = {}
		for metric in self.metrics:
			val = self.get_data(updated_data, self.queries[metric])
			if val is not None:
				val = float(val) / 1024 # bytes/s -> Kbytes/s
				self.data_sources[metric].stream(dict(time=time, display_time=display_time, data=[val]))


class LagsPlot(TimeSeriesPlot):
	metrics = ['max', 'min', 'mean']
	queries = {'max': 'lags#max', 'min': 'lags#min', 'mean': 'lags#mean'}
	requests = ['lags']
	line_colors = {'max': 'plum', 'min': 'palegoldenrod', 'mean': 'lightcoral'}

	def __init__(self):
		super(LagsPlot, self).__init__(self.metrics, self.queries, self.requests, self.line_colors)

	def create_plot(self):
		return super(LagsPlot, self).create_plot('Offset Lags',
												 yaxis_label='Offset Lags')


class VmPlot(TimeSeriesPlot):
	metrics = ['vm']
	queries = {'vm': 'vm'}
	requests = ['vm']
	line_colors = {'vm': 'lightseagreen'}

	def __init__(self):
		super(VmPlot, self).__init__(self.metrics, self.queries, self.requests, self.line_colors)

	def create_plot(self):
		return super(VmPlot, self).create_plot('Number of VMs', height=180,
											   yaxis_label='Number of VMs')


class MsgsizePlot(TimeSeriesPlot):
	metrics = ['msgsize']
	queries = {'bytesin': 'jmx#bytesin_1min', 'msgsin': 'jmx#msgsin_1min'}
	requests = ['jmx']
	line_colors = {'msgsize': 'skyblue'}

	def __init__(self):
		super(MsgsizePlot, self).__init__(self.metrics, self.queries, self.requests, self.line_colors)

	def create_plot(self):
		return super(MsgsizePlot, self).create_plot('Message Size', height=180,
													yaxis_label='Message Size [bytes]')

	def update_plot(self, time, display_time, updated_data):
		bytesin = super(MsgsizePlot, self).get_data(updated_data, self.queries['bytesin'])
		msgsin = super(MsgsizePlot, self).get_data(updated_data, self.queries['msgsin'])

		metric = self.metrics[0]
		if 0.0 < msgsin and 0.0 <= bytesin:
			val = bytesin / msgsin
			self.data_sources[metric].stream(dict(time=time, display_time=display_time, data=[val]))


