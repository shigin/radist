import os, time
from radist import mux_fds

fdlist = []
args = ['first', 'second', '3rd']
for arg in args:
    fdlist.append(os.popen2('./mux_test.sh %s' % arg)[1])
     
mr = mux_fds.MuxReader(fdlist)
nums = {}
for line in mr:
    if line == '':
        continue
    name, arg, num = line.split()
    assert arg in args
    if not nums.has_key(num):
        nums[num] = []
    nums[num].append(arg)

from pprint import pprint
#pprint(map(len, nums.values()))
#pprint(filter(lambda x: x != len(args), map(len, nums.values())))
for arg in args:
    assert len(filter(lambda x: arg in x, nums.values())) == len(nums)
assert len(filter(lambda x: x != len(args), map(len, nums.values()))) == 0
