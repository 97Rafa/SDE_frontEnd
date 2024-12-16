import datetime
import time

y = datetime.datetime.now()
print(y.strftime("%x %X"))

time.sleep(5)

x = datetime.datetime.now()
print(x.strftime("%x %X"))

if x>y:
    print('x is newest')
else:
    print('y is newest')