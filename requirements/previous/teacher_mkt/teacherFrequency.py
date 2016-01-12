# -*- coding: utf-8 -*-

# 截止12月19日

{1: 9144, 2: 3028, 3: 1654, 4: 1131, 5: 764, 6: 574, 7: 482, 8: 417, 9: 314, 10: 268, 11: 213, 12: 198, 13: 164, 14: 143, 15: 127, 16: 117, 17: 99, 18: 73, 19: 84, 20: 65, 21: 51, 22: 59, 23: 39, 24: 33, 25: 37, 26: 29, 27: 29, 28: 34, 29: 34, 30: 21, 31: 16, 32: 16, 33: 15, 34: 11, 35: 17, 36: 10, 37: 7, 38: 10, 39: 14, 40: 9, 41: 7, 42: 7, 43: 9, 44: 5, 45: 1, 46: 4, 47: 3, 48: 0, 49: 1, 50: 0, 51: 3, 52: 1, 53: 1, 54: 2, 55: 2, 56: 2, 57: 0, 58: 1, 59: 2, 60: 1, 61: 1, 62: 1, 63: 0, 64: 1, 65: 1, 66: 0, 67: 0, 68: 0, 69: 0, 70: 0, 71: 1, 72: 1, 73: 0, 74: 1, 75: 0, 76: 0, 77: 0, 78: 0, 79: 0, 80: 0, 81: 0, 82: 0, 83: 0, 84: 0, 85: 0, 86: 0, 87: 0, 88: 0, 89: 0, 90: 0, 91: 0, 92: 0, 93: 0, 94: 0, 95: 0, 96: 0, 97: 0, 98: 0, 99: 0, 100: 0, 101: 0, 102: 0, 103: 0, 104: 0, 105: 0, 106: 0, 107: 0, 108: 0, 109: 0, 110: 0}


# from 2015-09-10 00:00:00 to 2015-09-17 00:00:00.
# Average: 1.70 | Median: 1.00
# from 2015-09-17 00:00:00 to 2015-09-24 00:00:00.
# Average: 1.76 | Median: 1.00
# from 2015-09-24 00:00:00 to 2015-10-01 00:00:00.
# Average: 1.65 | Median: 1.00
# from 2015-10-01 00:00:00 to 2015-10-08 00:00:00.
# Average: 1.61 | Median: 1.00
# from 2015-10-08 00:00:00 to 2015-10-15 00:00:00.
# Average: 1.88 | Median: 1.00
# from 2015-10-15 00:00:00 to 2015-10-22 00:00:00.
# Average: 1.78 | Median: 1.00
# from 2015-10-22 00:00:00 to 2015-10-29 00:00:00.
# Average: 1.89 | Median: 1.00
# from 2015-10-29 00:00:00 to 2015-11-05 00:00:00.
# Average: 1.79 | Median: 1.00
# from 2015-11-05 00:00:00 to 2015-11-12 00:00:00.
# Average: 1.73 | Median: 1.00
# from 2015-11-12 00:00:00 to 2015-11-19 00:00:00.
# Average: 1.77 | Median: 1.00
# from 2015-11-19 00:00:00 to 2015-11-26 00:00:00.
# Average: 1.88 | Median: 1.00
# from 2015-11-26 00:00:00 to 2015-12-03 00:00:00.
# Average: 1.85 | Median: 1.00
# from 2015-12-03 00:00:00 to 2015-12-10 00:00:00.
# Average: 1.90 | Median: 1.00
# from 2015-12-10 00:00:00 to 2015-12-17 00:00:00.
# Average: 1.90 | Median: 1.00
# from 2015-12-17 00:00:00 to 2015-12-24 00:00:00.
# Average: 1.22 | Median: 1.00


import sys
import datetime
import numpy as np
reload(sys)
sys.setdefaultencoding('utf-8')
from pymongo import MongoClient, DESCENDING
from bson.objectid import ObjectId

db = MongoClient('10.8.8.111:27017')['onionsBackupOnline']
cacheDb = MongoClient('10.8.8.111:27017')['cache']
oldDb = MongoClient('10.8.8.111:27017')['miner-prod25']
teacherDb = MongoClient('10.8.8.111:27017')['teacher25temp']

events = db['events']
users = db['users']
userAttr = cacheDb['userAttr']

oldUsers = oldDb['users']
oldRooms = oldDb['rooms']
# points = oldDb['points']
sepActTeacherPoints = oldDb['sepActTeacherUser']
points = teacherDb['points']
tempTeacherWithCount = teacherDb['count']

# TODO: change date to 2015, 9, 1
sepDate = datetime.datetime(2015, 9, 10)
finalDate = datetime.datetime(2015, 12, 19)

# # remove id append one new list
# def resetAsList(userList):
#     users = []
#     for user in userList:
#         users.append(user['_id'])
#     return users
#
# # find all teacher user
# def findTeacherUser():
#     pipeLine = [
#         {"$match": {
#             "role": "teacher"
#         }},
#         {"$group": {
#             "_id": "$_id"
#         }}
#     ]
#     return list(users.aggregate(pipeLine))
#
# def activityTeacherAfterSep(userList, startDate):
#     pipeLine = [
#         {"$match": {
#             "createdBy": {
#                 "$gte": startDate,
#             },
#             "user": {"$in": userList}
#         }},
#         {"$group": {
#             "_id": "$user"
#         }}
#     ]
#     return list(points.aggregate(pipeLine))
#
# # activityTeacherAfterSepIdList = activityTeacherAfterSep(totalTeacherList, sepDate)
# # activityTeacherAfterSepList = resetAsList(activityTeacherAfterSepIdList)
# # print("9月后活跃过的教师用户:")
# # print(len(activityTeacherAfterSepList))
#
# print("------------level 2.-----------")
#
# # teacherDict = []
# # def resetAsDict(userList):
# #     teacherWithCount = {}
# #     for user in userList:
# #         # teacherWithCount['_id'] = ObjectId()
# #         teacherWithCount['teacherId'] = user
# #         teacherWithCount['counter'] = 0
# #         teacherDict.append(teacherWithCount)
# #
# # # [{'count': 0, '_id': ObjectId('55fe081620d7d67d241c74ef')}, {'count': 0, '_id': ObjectId('55fe081620d7d67d241c74ef')}]
# # resetAsDict(activityTeacherAfterSepList)
# #
# # for user in teacherDict:
# #     user['_id'] = ObjectId()
# #     tempTeacherWithCount.insert_one(user)
#
# # [{u'teacherId': ObjectId('55fe081620d7d67d241c74ef'), u'_id': ObjectId('5694a55fe7d496ba6f6825e2'), u'counter': 0},]
# teacherDict = tempTeacherWithCount.find()
# teacherDictList = list(teacherDict)
#
# # remove id append one new list
# def resetAsActTeacherList(userList):
#     users = []
#     for user in userList:
#         users.append(user['teacherId'])
#     return users
#
# actTeacherDictList = resetAsActTeacherList(teacherDictList)
# print("------------level 3.-----------")
#
# def isActivityThisDay(userList, startDate, endDate):
#     actToday = []
#     pipeLine = [
#         {"$match": {
#             "createdBy": {
#                 "$gte": startDate,
#                 "$lt": endDate
#             },
#             "user": {"$in": userList}
#         }},
#         {"$group": {
#             "_id": "$user"
#         }}
#     ]
#     actTeacher = list(points.aggregate(pipeLine))
#
#     actToday.extend(actTeacher)
#
#     pipeLine = [
#         {"$match": {
#             "teacherId": {"$in": actToday}
#         }},
#         {"$group": {
#             "counter": {"$inc": 1}
#         }}
#     ]
#     tempTeacherWithCount.aggregate(pipeLine)
#
# dateNow = sepDate
# while dateNow != finalDate:
#     startDate = dateNow
#     endDate = startDate + datetime.timedelta(days=1)
#     dateNow = endDate
#     print("from %s to %s.")%(startDate, endDate)
#     isActivityThisDay(actTeacherDictList, startDate, endDate)
#
# print(len(teacherDictList))

def calc_daily_active(start, users, end_day):
    global teacher_counter,x
    # cur_start = start

    pipeline = [
        {"$match":
             {
                 "createdBy": {"$gte": start, "$lt": end_day},
                 "user": {"$in": users}
        }},
        {"$project": {"dayOrder": {"$dayOfYear": "$createdBy"}, "user": 1}},
        {"$group": {
            "_id": "$dayOrder",
            "users": {"$addToSet": "$user"}
        }}
    ]
    dailies = list(points.aggregate(pipeline))
    for each in dailies:
        # print each["_id"]
        for unit_user in each['users']:
            teacher_counter[unit_user] += 1


    # while cur_start <= end_day:
    #     print cur_start
    #     end = cur_start + datetime.timedelta(days=1)
    #     actTeacher = points.find({
    #         "createdBy": {
    #             "$gte": start,
    #             "$lt": end
    #         },
    #         "user": {"$in": users}}, {"user": 1})
    #
    #     daily_teachers = list(set([each['user'] for each in actTeacher]))
    #     for each in daily_teachers:
    #         teacher_counter[each] += 1
    #
    #     cur_start = end

def calc_dist(days):
    global teacher_counter
    count_dist = dict((el, 0) for el in range(days+1))
    for each in teacher_counter.values():
        count_dist[each] += 1
    return count_dist

# teachers = oldUsers.find({"role": "teacher"}, {"_id": 1})
# teachers = [each['_id'] for each in list(teachers)]
# teacher_counter = dict((el,0) for el in teachers)
# x = ""
# start = datetime.datetime(2015,9,1)

# tc_back = dict(teacher_counter)

# calc_daily_active(start, teachers, start + datetime.timedelta(days=7))
# y = calc_dist(7)
# y[0] = 0
# days_dist = []
# for k, v in y.iteritems():
#     days_dist += [k] * v
# days_dist =np.array(days_dist)
# average = days_dist.mean()
# median = np.median(days_dist)
# print average, median

dateNow = sepDate
while dateNow <= finalDate:
    startDate = dateNow
    endDate = startDate + datetime.timedelta(days=7)
    dateNow = endDate
    print("from %s to %s.")%(startDate, endDate)

    teachers = oldUsers.find({"role": "teacher"}, {"_id": 1})
    teachers = [each['_id'] for each in list(teachers)]
    teacher_counter = dict((el,0) for el in teachers)
    x = ""

    tc_back = dict(teacher_counter)
    calc_daily_active(startDate, teachers, endDate)
    y = calc_dist(7)
    y[0] = 0
    days_dist = []
    for k, v in y.iteritems():
        days_dist += [k] * v
    days_dist =np.array(days_dist)
    average = days_dist.mean()
    median = np.median(days_dist)
    # print average, median
    print("Average: %.2f | Median: %.2f")%(average, median)

