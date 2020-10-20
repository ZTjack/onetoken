'''
Author: Jack
Date: 2020-10-20 13:42:57
LastEditors: Jack
LastEditTime: 2020-10-20 15:10:14
Description: 
'''
# test_list = [{
#     "id": 1,
#     "data": "HappY"
# }, {
#     "id": 2,
#     "data": "BirthDaY"
# }, {
#     "id": 3,
#     "data": "Rash"
# }]

# # printing original list
# print("The original list is : " + str(test_list))

# # using del + loop
# # to delete dictionary in list
# test_list[:] = [order for order in test_list if order['id'] > 1]
import time

# import logging
# logging.basicConfig(filename='example.log',
#                     format='%(levelname)s:%(message)s',
#                     level=logging.DEBUG)
# logging.debug('This message should appear on the console')
# logging.info('So should this')
# logging.warning('And this, too')
# print("%s", % (time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))
print('Hi, %s, you have $%d.' % ('Michael', 1000000))


def check_signal():
    while True:
        print('Start Loop Tick')
        time.sleep(2)


# check_signal()