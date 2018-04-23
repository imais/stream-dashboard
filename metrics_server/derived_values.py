import numpy as np
import sys
from datetime import datetime


class OneMinuteRate(object):
	def __init__(self):
		self.history = []	

	def add_history(self, time, val):
		self.history.append((time, val))

	def compute_one_minute_rate(self):
		# Compute one-minute rate
		now = datetime.now()
		self.history[:] = [(time, val) for (time, val) in self.history 
						   if (now - time).total_seconds() <= 60.0]
		# print('### history={}'.format(self.history))
		total = sum([val for (time, val) in self.history])
		# print('### total={}'.format(total))
		return (float)(total) / 60.0


class PartitionsDerivedValue(OneMinuteRate):
	def __init__(self, var, partition_field):
		super(PartitionsDerivedValue, self).__init__()
		self.partition_field = partition_field
		self.var = var
		self.last_time = None
		self.last_val = None

	def compute(self, partitions):
		# partitions = {"partition_0": {"tail": 1435, "lag": 48, "commited": 1387}, 
		#               "partition_1": {"tail": 1429, "lag": 0, "commited": 1429}, ...}
		empty_result = (self.var, {self.var: 0.0, self.var + '_1min': 0.0})

		if partitions is None or partitions == {}:
			return empty_result

		val = sum([partitions[p][self.partition_field] for p in partitions])
		now = datetime.now()
		if self.last_val is None or self.last_time is None or now <= self.last_time:
			self.last_val = val
			self.last_time = now
			return empty_result

		# Compute per-second change per time
		delta_val = val - self.last_val
		rate = delta_val / (now - self.last_time).total_seconds()
		# print("#### last_val={}, val={}, delta_val={}, delta_sec={}".
		# 	  format(self.last_val, val, delta_val, (now - self.last_time).total_seconds()))
		self.last_val = val
		self.last_time = now
		self.add_history(now, delta_val)
		min_rate = self.compute_one_minute_rate()

		return (self.var, {self.var: rate, self.var + '_1min': min_rate})


class MsgsIn(PartitionsDerivedValue):
	def __init__(self):
		super(MsgsIn, self).__init__('msgsin', 'latest')


class MsgsOut(PartitionsDerivedValue):
	def __init__(self):
		super(MsgsOut, self).__init__('msgsout', 'committed')


class OffsetLags(object):
	def compute(self, partitions):
		if partitions is None or partitions == {}:
			return ('lags', {'min': 0.0, 'max': 0.0, 'mean': 0.0, 'num': 0})

		lags = [partitions[p]['lag'] for p in partitions]
		stats = {'max': max(lags), 'min': min(lags), 'mean': np.mean(lags), 'num': len(partitions)}

		return ('lags', stats)


class WaitTime(object):
	_latest = {}
	_committed = {}

	def compute(self, partitions):
		delta_timestamps = np.array([])

		latest = self._latest
		committed = self._committed

		for p in partitions:
			latest_offset = partitions[p]['latest']
			committed_offset = partitions[p]['committed']
			timestamp = partitions[p]['timestamp']

			# init latest & committed
			if not latest.has_key(p):
				latest[p] = {'offset': np.array([]), 'timestamp': np.array([])}
			if not committed.has_key(p):
				committed[p] = {'offset': np.array([]), 'timestamp': np.array([])}

			if (latest_offset is not None) and (timestamp is not None):
				latest[p]['offset'] = np.append(latest[p]['offset'], latest_offset)
				latest[p]['timestamp'] = np.append(latest[p]['timestamp'], timestamp)
			if (committed_offset is not None) and (timestamp is not None):
				committed[p]['offset'] = np.append(committed[p]['offset'], committed_offset)
				committed[p]['timestamp'] = np.append(committed[p]['timestamp'], timestamp)

			if len(latest[p]['offset']) <= 1 or len(committed[p]['offset']) <= 1:
				continue

			# there are at least more than 1 elements both in latest and committed
			latest_offset_range = np.arange(latest[p]['offset'][-2], latest[p]['offset'][-1] + 1)
			# print('latest[p][offset][-1]={}, latest[p][offset][-2]={}'.format(latest[p]['offset'][-1], latest[p]['offset'][-2]))
			latest_interp = np.interp(latest_offset_range,
									  [latest[p]['offset'][-2], latest[p]['offset'][-1]],
									  [latest[p]['timestamp'][-2], latest[p]['timestamp'][-1]])
			latest[p]['offset'] = np.append(latest[p]['offset'][:-2], latest_offset_range)
			latest[p]['timestamp'] = np.append(latest[p]['timestamp'][:-2], latest_interp)

			committed_offset_range = np.arange(committed[p]['offset'][-2], committed_offset + 1)
			committed_interp = np.interp(committed_offset_range,
										 [committed[p]['offset'][-2], committed_offset],
										 [committed[p]['timestamp'][-2], timestamp])
			committed[p]['offset'] = np.append(committed[p]['offset'][:-2], committed_offset_range)
			committed[p]['timestamp'] = np.append(committed[p]['timestamp'][:-2], committed_interp)

			# find common offsets from committed and latest
			min_committed = committed[p]['offset'][0]
			max_committed = committed[p]['offset'][-1]
			common_offsets = latest[p]['offset'][(min_committed <= latest[p]['offset']) & (latest[p]['offset'] <= max_committed)]
			# print('### Before uptates')
			# print('### latest[p][offset]={}'.format(latest[p]['offset']))
			# print('### committed[p][offset]={}'.format(committed[p]['offset']))
			# print('### common_offsets={}'.format(common_offsets))

			latest_timestamps = latest[p]['timestamp'][(common_offsets[0] <= latest[p]['offset']) & (latest[p]['offset'] <= common_offsets[-1])]
			committed_timestamps = committed[p]['timestamp'][(common_offsets[0] <= committed[p]['offset']) & (committed[p]['offset'] <= common_offsets[-1])]
			delta = [(committed_timestamps[i] - latest_timestamps[i]) for i in range(0, len(committed_timestamps))]
			delta_timestamps = np.append(delta_timestamps, delta)
			# print('### delta_timestamps={}'.format(delta_timestamps))
			
			# remove processed offsets info
			filter = common_offsets[-1] <= latest[p]['offset']
			latest[p]['offset'] = latest[p]['offset'][filter]
			latest[p]['timestamp'] = latest[p]['timestamp'][filter]
			filter = common_offsets[-1] <= committed[p]['offset']
			committed[p]['offset'] = committed[p]['offset'][filter]
			committed[p]['timestamp'] = committed[p]['timestamp'][filter]

			# print('### After updates')
			# print('### latest[p][offset]={}'.format(latest[p]['offset']))
			# print('### committed[p][offset]={}'.format(committed[p]['offset']))

		return ('wait_time', np.mean(delta_timestamps) if 0 < len(delta_timestamps) else None)

			

				
						   

				
				
				
				
											
			
		
		
	
	
