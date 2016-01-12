# -*- coding: utf-8 -*-
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

def totalTeacher():
    pipeLine = [
        {"$match": {
            "role": "teacher"
        }},
        {"$project": {
            "_id": "$_id"
        }}
    ]
    return list(users.aggregate(pipeLine))

print("教师用户总数:")
print(len(totalTeacher()))

def teacherWhoHasClassRoom():
    pipeLine = [
        {"$match": {
            "role": "teacher",
            "rooms": {"$not": {"$size": 0}}
        }},
        {"$project":{
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

def pickRoomIdThenFindStudent(roomsIdList):
    pipeLine = [
        {"$match": {
            "rooms": {"$elemMatch": {"$in": roomIdList}}
        }},
        {"$project": {
            "_id": "$_id"
        }}
    ]
    return list(users.aggregate(pipeLine))

print("批量创建学生用户数:")
batchStudents = pickRoomIdThenFindStudent(roomIdList)
print(len(batchStudents))

def isActivistUser(userList):
    pipeLine = [
        {"$match": {
            "user": {"$in": userList}
        }},
        {"$group": {
            "_id": "$user"
        }}
    ]
    return list(events.aggregate(pipeLine))

actStuList = []
for oneStu in batchStudents:
    actStuList.append(oneStu['_id'])

print("批量创建用户多少人激活:")
activistStudents = isActivistUser(actStuList)
print(len(activistStudents))

# calculate daterange after 20150901 teacher

print("9月1日后活跃过的有班级教师用户总数:")

def teacherActAfter9(teacherWhoHasClassRoomList):
    teacherList = []
    for teacher in teacherWhoHasClassRoomList:
        teacherList.append(teacher['_id'])

    startDate = datetime.datetime(2014, 1, 1)
    pipeLine = [
        {"$match": {
            "user": {"$in": teacherList},
            "serverTime": {"$gte": startDate}
        }},
        {"$group": {
            "_id": "$user"
        }}
    ]
    print len(list(events.aggregate(pipeLine)))

teacherActAfter9(teacherWhoHasClassRoomList)

print("9月1日后活跃过的批量创建学生用户总数:")
def activistTeacher():
    pipeLine = [
        {"$match": {
            "role": "teacher",
        }},
        {"$project": {
            "_id": 1
        }}
    ]
    print list(users.aggregate(pipeLine))

def activistTeacherWithDateRange(startDate):
    pipeLine = [
        {"$match": {
            "role": "teacher",
            "dailySignIn.updateTime": {"$gte": startDate},
            "dailySignIn.longestStreak": {"$gte": 13},
            "dailySignIn.streak": {"$gte": 1}
        }},
        {"$project": {
            "_id": 1
        }}
    ]
    print list(users.aggregate(pipeLine))

activistTeacher()
activistTeacherWithDateRange(START_DATE)


# find teachers Id and roomId
# return teachers Id list and roomId list
#
# input usersId list
# output userId list
def activistOrNot(userIdList):
    actUser = []
    pipeLine = [
        {"$match": {
            "user": {"$in": userIdList}
        }},
        {"$group": {
            "_id": {"$addToSet": "$_id"}
        }}
    ]
    return list(events.aggregate(pipeLine))

