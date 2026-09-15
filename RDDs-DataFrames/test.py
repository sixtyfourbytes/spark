import collections

result = {'3': 2200, '2': 888, '1': 8393, '4': 1992}

result = collections.OrderedDict(sorted(result.items()))
print(result)
print(sorted(result.items()))
