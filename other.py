
# import pandas as pd
# # Create two Pandas Series from list
# ser1 = pd.Series(['python', 'php', 'java'])
# # print(ser1)
# ser2 = pd.Series(['Spark', 'PySpark', 'Pandas'])
# # print(ser2)
# append_ser = ser1._append(ser2)
# # print(append_ser)

# l = list(append_ser)
# print(l)
# print(type(l))
# c = ",".join(l)
# print(c)

# t = "My name is '{arif}'"\
#     +"and name is {arif}".format(arif="ansari")
# print(t)

# from datetime import datetime
# print(datetime.today().strftime('%Y-%m-%d %H:%M:%S.%f'))

import os
for f in os.listdir('./view'):
    print(f)