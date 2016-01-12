# -*- coding: utf-8 -*-

# 教师用户质量调查
# teacherWhoHasClassRoom() 计算有班级的教师总数
# pickRoomIdThenFindStudent() 教师名下学生总数
# isUserActivity() 教师名下活跃用户总数

# 截止1月9日
# 拥有班级的教师用户:
# 5425
# 批量创建学生用户数:
# 298573
# 教师名下活跃学生总数:
# 60792
# 9月份后活跃的教师用户(有班级):
# 3237
# 9月份后活跃教师名下的活跃学生总数:
# 54104

import sys
import datetime
reload(sys)
sys.setdefaultencoding('utf-8')
from pymongo import MongoClient, DESCENDING
from bson.objectid import ObjectId

db = MongoClient('10.8.8.111:27017')['onionsBackupOnline']
cacheDb = MongoClient('10.8.8.111:27017')['cache']
oldDb = MongoClient('10.8.8.111:27017')['miner-prod25']

events = db['events']
users = db['users']
userAttr = cacheDb['userAttr']

oldUsers = oldDb['users']
oldRooms = oldDb['rooms']
points = oldDb['points']

START_DATE = datetime.datetime(2015, 9, 1)
END_DATE = datetime.datetime(2015, 12, 18)

# 找出所有拥有班级的教师用户
# input null
# output [{"_id":ObjectId("..."), "rooms":[]}]
def teacherWhoHasClassRoom():
    pipeLine = [
        {"$match": {
            "role": "teacher",
            "rooms": {"$not": {"$size": 0}}
        }},
        {"$project": {
            "_id": 1,
            "rooms": 1
        }}
    ]
    return list(users.aggregate(pipeLine))

print("拥有班级的教师用户:")
teacherWhoHasClassRoomList = teacherWhoHasClassRoom()
print(len(teacherWhoHasClassRoomList))

roomIdList = []
for user in teacherWhoHasClassRoomList:
    roomIdList.extend(user['rooms'])

# 找出所有加入班级的用户
# input roomId list [ObjectId("..."), ObjectId("..."), ...]
# output [{"_id": ObjectId("...")}]
def pickRoomIdThenFindStudent(roomsIdList):
    pipeLine = [
        {"$match": {
            "rooms": {"$elemMatch": {"$in": roomIdList}},
            "role": "student"
        }},
        {"$project": {
            "_id": "$_id"
        }}
    ]
    return list(users.aggregate(pipeLine))

print("批量创建学生用户数:")
batchStudents = pickRoomIdThenFindStudent(roomIdList)
print(len(batchStudents))

activityUserList = []
for user in batchStudents:
    activityUserList.append(user['_id'])

# 判断用户是否活跃
# input userList as [ObjectId("..."), ObjectId("...")]
# output [{"_id": ObjectId("...")}, {"_id": ObjectId(""), ...}]
def isUserActivity(userList):
    pipeLine = [
        {"$match": {
            "user": {"$in": userList}
        }},
        {"$group": {
            "_id": "$user"
        }}
    ]
    return list(points.aggregate(pipeLine))

print("教师名下活跃学生总数:")
print len(isUserActivity(activityUserList))

# 9月1日后
# 活跃过的有班级的用户总数
# 教师名下活跃学生总数

# 判断一组用户九月后是否活跃
# input userList datetime
# output ["_id": ObjectId("..."), ...]
def isUserActivityAfterSep(userList, startDate):
    pipeLine = [
        {"$match": {
            "createdBy": {
                "$gte": startDate
            },
            "user": {"$in": userList}
        }},
        {"$group": {
            "_id": "$user"
        }}
    ]
    return list(points.aggregate(pipeLine))

sepTeacherList = []
for teacher in teacherWhoHasClassRoomList:
    sepTeacherList.append(teacher['_id'])

print("9月份后活跃的教师用户(有班级):")
sepTeacherList = isUserActivityAfterSep(sepTeacherList, START_DATE)
print(len(sepTeacherList))

def teacherWhoHasClassRoomAfterSep(userList, startDate):
    usersPipe = []
    for user in userList:
        usersPipe.append(user['_id'])
    pipeLine = [
        {"$match": {
            "serverTime": {"$gte": startDate},
            "_id": {"$in": usersPipe}
        }},
        {"$project": {
            "_id": "$_id",
            "rooms": "$rooms"
        }}
    ]
    return list(users.aggregate(pipeLine))

sepTeacherWithRoomList = teacherWhoHasClassRoomAfterSep(sepTeacherList, START_DATE)

# Pick up room
roomLists = []
for teacher in sepTeacherWithRoomList:
    roomIdList.extend(teacher['rooms'])

def findUsersByRoomId(roomList):
    pipeLine = [
        {"$match": {
            "rooms": {"$elemMatch": {"$in": roomList}},
            "role": "student"
        }},
        {"$group": {
            "_id": "$_id"
        }}
    ]
    return list(users.aggregate(pipeLine))

sepBatchUserList = findUsersByRoomId(roomIdList)

sepBatchUserIdList = []
for user in sepBatchUserList:
    sepBatchUserIdList.append(user['_id'])

sepActBatchUserList = isUserActivityAfterSep(sepBatchUserIdList, START_DATE)
print("9月份后活跃教师名下的活跃学生总数:")
print len(sepActBatchUserList)
