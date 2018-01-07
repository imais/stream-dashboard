import numpy as np
from datetime import datetime
from bokeh.models import ColumnDataSource, HoverTool, Legend, SaveTool
from bokeh.plotting import figure


class TimeSeriesPlot(object):
	tools = 'pan, xbox_zoom, save, reset'
	hover = HoverTool(tooltips=[("Time", "@display_time"), ("Data", "@data")])
	x_range = None
	default_width  = 650
	default_height = 200

	def __init__(self, metrics, line_colors, line_dashes=None):
		self.metrics = metrics
		self.line_colors = line_colors
		self.line_dashes = line_dashes
		self.data_sources = {}

	def create_plot(self, title, width=default_width, height=default_height, 
					xaxis_label='Time', yaxis_label='Data', line_width=3):
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
				r = p.line(source=self.data_sources[metric], x='time', y='data', color=self.line_colors[metric], line_dash=self.line_dashes[metric], line_width=3, muted_color=self.line_colors[metric], muted_alpha=0.2)
			else:
				r = p.line(source=self.data_sources[metric], x='time', y='data', color=self.line_colors[metric], line_width=3, muted_color=self.line_colors[metric], muted_alpha=0.2)
			if 1 < len(self.metrics):
				legend_items.append((metric, [r]))
		legend = Legend(items=legend_items, click_policy='mute')
			
		p.y_range.start = 0
		p.xaxis.axis_label = xaxis_label
		p.yaxis.axis_label = yaxis_label
		p.add_layout(legend, 'right')
		# p.legend.location = 'top_left'
		# p.legend.click_policy = 'mute'
		# p.legend.label_text_font_size = '8pt'
		p.add_tools(self.hover)

		self.p = p
		return p

	def update_plot(self, time, display_time, updated_data):
		for metric in self.metrics:
			if updated_data[metric] is not None:
				data = [float(updated_data[metric])]
				self.data_sources[metric].stream(dict(time=time, display_time=display_time, data=data))


class BytesPlot(TimeSeriesPlot):
	metrics = ['bytesout', 'bytesin', 'bytesout_1minavg', 'bytesin_1minavg']
	line_colors = {'bytesout': 'dodgerblue', 'bytesin': 'mediumseagreen', 'bytesout_1minavg': 'lightskyblue', 'bytesin_1minavg': 'mediumaquamarine'}
	line_dashes = {'bytesout': 'solid', 'bytesin': 'solid', 'bytesout_1minavg': 'dashed', 'bytesin_1minavg': 'dashed'}

	def __init__(self):
		super(BytesPlot, self).__init__(self.metrics, self.line_colors, self.line_dashes)

	def create_plot(self):
		return super(BytesPlot, self).create_plot('BytesOut/BytesIn',
												  yaxis_label='Data Rate [Kbytes/sec]')

	def update_plot(self, time, display_time, updated_data):
		for metric in self.metrics:
			if updated_data[metric] is not None:
				data = [float(updated_data[metric]) / 1024] # bytes/s -> Kbytes/s
				self.data_sources[metric].stream(dict(time=time, display_time=display_time, data=data))


class LagsPlot(TimeSeriesPlot):
	metrics = ['max', 'min', 'mean']
	line_colors = {'max': 'lightcoral', 'min': 'plum', 'mean': 'palegoldenrod'}

	def __init__(self):
		super(LagsPlot, self).__init__(self.metrics, self.line_colors)

	def create_plot(self):
		return super(LagsPlot, self).create_plot('Offset Lags',
												 yaxis_label='Offset Lags')

	def update_plot(self, time, display_time, updated_data):
		if updated_data['offsets'] is None or updated_data['offsets'] == []:
			return
		partitions = updated_data['offsets']
		lags = [p.values()[0]['lag'] for p in partitions]
		data = {'max': max(lags), 'min': min(lags), 'mean': np.mean(lags)}
		super(LagsPlot, self).update_plot(time, display_time, data)


class VmPlot(TimeSeriesPlot):
	metrics = ['vm']
	line_colors = {'vm': 'lightseagreen'}

	def __init__(self):
		super(VmPlot, self).__init__(self.metrics, self.line_colors)

	def create_plot(self):
		return super(VmPlot, self).create_plot('Number of VMs', height=180,
											   yaxis_label='Number of VMs')



class MsgsizePlot(TimeSeriesPlot):
	metrics = ['msgsize']
	line_colors = {'msgsize': 'skyblue'}

	def __init__(self):
		super(MsgsizePlot, self).__init__(self.metrics, self.line_colors)

	def create_plot(self):
		return super(MsgsizePlot, self).create_plot('Average Message Size', height=180,
													yaxis_label='Average Message Size [bytes]')

	def update_plot(self, time, display_time, updated_data):
		if updated_data['bytesin_1minavg'] is None or updated_data['msgsin_1minavg'] is None:
			return

		if 0.0 < updated_data['msgsin_1minavg']:
			data = {'msgsize': updated_data['bytesin_1minavg'] / updated_data['msgsin_1minavg']}
		else:
			data = {'msgsize': 0.0}
		super(MsgsizePlot, self).update_plot(time, display_time, data)


