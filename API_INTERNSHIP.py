from flask import Flask,jsonify,request
import psycopg2
import json
import jsonschema
from jsonschema import validate
app=Flask(__name__)


conn =0
def connectToDatabase():
    global conn
    conn = psycopg2.connect(database="test_db", user = "postgres", password = "root", host = "127.0.0.1", port = "5432")
    return conn.cursor()

def connectEnd(conn):
    conn.close()


def checkPhoneNumber(num):
    cur = connectToDatabase()
    cur.execute(f'''SELECT * from citizenrecords ''');
    result = cur.fetchall();
    cnt=0
    for comp in result:
        if comp[0]==num:
            cnt=1
            break
        else:
            cnt=0
    connectEnd(cur)        
    return cnt

userSchema = {
            "phoneNumber":{"type":"number"},
            "firstName":{"type":"string"},
            "lastName":{"type":"string"},
            "age":{"type":"number"},
            "gender":{"type":"string"},
            "lattitude":{"type":"number"},
            "longitude":{"type":"number"},
            "town":{"type":"string"}, 
            "pincode":{"type":"number"}
}

dataSchema = {
    "type": "object",
    "properties": {
            "phoneNumber":{"type":"number"},
            "firstName":{"type":"string"},
            "lastName":{"type":"string"},
            "age":{"type":"number"},
            "gender":{"type":"string"},
            "lattitude":{"type":"number"},
            "longitude":{"type":"number"},
            "town":{"type":"string"}, 
            "pincode":{"type":"number"}
    },
}

def checkType(jsonData):
    try:
        validate(instance=jsonData, schema=dataSchema)
    except jsonschema.exceptions.ValidationError as err:
        return False
    return True

def validateJson(jsonData):
    print(type(userSchema))
    for key in userSchema.keys():
        if not key in jsonData:
            print(key)
            return False
            
    return True
    

@app.route('/citizen/register',methods=['POST'])
def create_user():
    #First check whether you received the json object and it is well-formed
    
        try:
            if validateJson(request.get_json()) and checkType(request.get_json()): 
                if request.is_json:
                    reqData=request.get_json()
                    cnt=checkPhoneNumber(reqData['phoneNumber'])



                    if cnt==1:
                        
                        return "1001"

                    else:
                        # cur = connectToDatabase()

                        # #procCall = f"CALL {'insertCitizens'}('{reqData}')"
                        # procCall = f'''CALL {'registerUser'}({reqData['phoneNumber']},'{json.dumps(reqData)}')'''

                        # cur.execute(procCall)
                        # conn.commit()
                        # connectEnd(cur)

                        return "1003" 
                else:
                    return "1006"
            else:
                return "1004"    
        except:
            return "1005"



@app.route('/citizen/isregistered/<int:num>',methods=['GET'])
def check_citizen(num):
    try:
        cnt=checkPhoneNumber(num)

        if cnt==1:
            return  "1001"
        else:
            return "1002"
    except:
        return "1005"



@app.route('/citizen/demographicUpdate/<int:num>',methods=['POST'])
def demographicUpdate(num):
    try:    
        cnt=checkPhoneNumber(num)
        
        if cnt==1:
            if request.is_json:
                reqData = request.json
                cur = connectToDatabase()
                cur.execute(f'''SELECT * from citizenrecords where phonenumber = {num} ''');
                result = cur.fetchall()
                currentJson = result[0][1]
                currentJson.update(reqData) # updating current json file
                cur.execute(f'''CALL {'updateuser'}({num},'{json.dumps(currentJson)}')''');
                conn.commit()
                connectEnd(cur)
                return '1007'
            else:
                return '1006'
            
        else:
            return '1002'
    except:
        return "1005"



# @app.route('/citizen/occupation/<int:num>',methods=['POST'])
# def addOccupation(num):
#     cnt=checkPhoneNumber(num)

#     if cnt==1:
#         reqData=request.get_json()
#         newStore={
#             "phoneNumber":reqData['phoneNumber'],
#             "occupation":reqData['occupation']
#         }
#         jsonData = json.dumps({"phoneNumber":reqData['phoneNumber'],"occupation":newStore['occupation']})
#         cur = conn.cursor()
#         procCall = f"CALL {'addoccupation'}('{jsonData}')"
#         cur.execute(procCall)
#         conn.commit()

#         return jsonify("Done")
#     else:
#         return jsonify({"Message : ":"Not Registered!!!"})
    
#     return jsonify(newStore)

app.run(port=8000)