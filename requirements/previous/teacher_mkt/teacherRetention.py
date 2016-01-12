# -*- coding: utf-8 -*-

# 教师用户质量调查
# teacherWhoHasClassRoom() 计算有班级的教师总数
# pickRoomIdThenFindStudent() 教师名下学生总数
# isUserActivity() 教师名下活跃用户总数

# 截止1月9日
# --------------------------------
#
# 9月份曾经访问过学生数据的页面的用户:
# 557
# 这批9月用户 10月留存:
# 322
# 这批9月用户 11月留存:
# 252
# 这批9月用户 12月3.0上线前留存:
# 197
# --------------------------------
#
# --------------------------------
#
# 10月份曾经访问过学生数据的页面的用户:
# 523
# 这批10月用户 11月留存:
# 252
# 这批10月用户 12月3.0上线前留存:
# 197
# --------------------------------
#
# --------------------------------
#
# 11月份曾经访问过学生数据的页面的用户:
# 546
# 这批11月用户 12月3.0上线前留存:
# 197
# --------------------------------



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

sepDate = datetime.datetime(2015, 9, 1)
octDate = datetime.datetime(2015, 10, 1)
novDate = datetime.datetime(2015, 11, 1)
decDate = datetime.datetime(2015, 12, 1)

def activityInThisMonth(startDate):
    endDate = startDate + datetime.timedelta(days=30)
    pipeLine = [
        {"$match": {
            "createdBy": {
                "$gte": startDate,
                "$lt": endDate
            },
            "eventKey": "enterClassSurvey"
        }},
        {"$group": {
            "_id": "$user"
        }}
    ]
    return list(points.aggregate(pipeLine))

def resetAsList(userList):
    users = []
    for user in userList:
        users.append(user['_id'])
    return users

def retentionOrNot(userList, startDate):
    endDate = startDate + datetime.timedelta(days=30)
    pipeLine = [
        {"$match": {
            "createdBy": {
                "$gte": startDate,
                "$lte": endDate
            },
            "user": {"$in": userList}
        }},
        {"$group": {
            "_id": "$user"
        }}
    ]
    return list(points.aggregate(pipeLine))

def printLine():
    print("--------------------------------\n")

printLine()
print("9月份曾经访问过学生数据的页面的用户:")
sepUsersOriginList = activityInThisMonth(sepDate)
print(len(sepUsersOriginList))

sepUsersIdList = resetAsList(sepUsersOriginList)

print("这批9月用户 10月留存:")
print(len(retentionOrNot(sepUsersIdList, octDate)))
print("这批9月用户 11月留存:")
print(len(retentionOrNot(sepUsersIdList, novDate)))
print("这批9月用户 12月3.0上线前留存:")
print(len(retentionOrNot(sepUsersIdList, decDate)))
printLine()

printLine()
print("10月份曾经访问过学生数据的页面的用户:")
octUsersOriginList = activityInThisMonth(octDate)
print(len(octUsersOriginList))

octUsersIdList = resetAsList(octUsersOriginList)

print("这批10月用户 11月留存:")
print(len(retentionOrNot(octUsersIdList, novDate)))
print("这批10月用户 12月3.0上线前留存:")
print(len(retentionOrNot(octUsersIdList, decDate)))
printLine()

printLine()
print("11月份曾经访问过学生数据的页面的用户:")
novUsersOriginList = activityInThisMonth(novDate)
print(len(novUsersOriginList))

novUsersIdList = resetAsList(novUsersOriginList)

print("这批11月用户 12月3.0上线前留存:")
print(len(retentionOrNot(novUsersIdList, decDate)))
printLine()
