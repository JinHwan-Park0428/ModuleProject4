from datetime import datetime

print(int(datetime.strftime(datetime.now(), "%Y")) + 1)

x = "19990101"

print(type(int(x[0:4])))
