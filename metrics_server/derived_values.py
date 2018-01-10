import numpy as np
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
		super(MsgsIn, self).__init__('msgsin', 'tail')


class MsgsOut(PartitionsDerivedValue):
	def __init__(self):
		super(MsgsOut, self).__init__('msgsout', 'commited')


class OffsetLags(object):
	def compute(self, partitions):
		if partitions is None or partitions == {}:
			return ('lags', {'min': 0.0, 'max': 0.0, 'mean': 0.0, 'num': 0})

		lags = [partitions[p]['lag'] for p in partitions]
		stats = {'max': max(lags), 'min': min(lags), 'mean': np.mean(lags), 'num': len(partitions)}

		return ('lags', stats)
