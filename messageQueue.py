import thread
import threading
import time
import sys, traceback
import StringIO
import cgitb
from xml.dom.minidom import Document
import copy
import socket
import datetime
import urllib2
import urllib
import simplejson
import parseconfig
from core import model,RISmodel
import xml2Dict
import xmltodict
from logconf import *
from webappconfig import *
import xml2Dict_Dict2xml
from core import util
import json
from RISPACSModel.Schema_DDL import SQLOperators
radconfig=parseconfig.RadConfig().config
try:
    messagePollTime=int(radconfig['APPLICATION']['MessagePollTime'])
except Exception,e:
    log.error("Error in getting messagepollout. Setting messagepollout to default")
    messagePollTime=5
try:
    socketTimeOut=int(radconfig['APPLICATION']['SocketTimeOut'])
except Exception,e:
    log.error("Error in getting socket time out. Setting socket time out to default")
    socketTimeOut=10
    
#socket.setdefaulttimeout(socketTimeOut)
class HttpBot:
    """an HttpBot represents one browser session, with cookies."""
    def __init__(self):
        cookie_handler= urllib2.HTTPCookieProcessor()
        redirect_handler= urllib2.HTTPRedirectHandler()
        self._opener = urllib2.build_opener(redirect_handler, cookie_handler)

    def GET(self, url):
        return self._opener.open(url).read()

    def POST(self, url, parameters):
        return self._opener.open(url, urllib.urlencode(parameters)).read()
        
__EventDescription__ = {'NEW_USER':'The admin created the new user',
                    'EDIT_USER': 'The admin has changed the user details',
                    'ADD_OR_UPDATE_USER': 'The admin has created or changed the user details',
                    'ENABLE_DISABLE_USER': 'The admin has changed the status of enable/disable for user.',
                    'DISABLE_USER':'The admin has disabled the user',
                    'CHANGE_PASSWD':'The user password is changed',
                    'ENABLE_USER':'The admin has enabled user',
                    'DELETE_USER':'The admin has deleted  user',
                    'DISABLE_USER_FROM_HOSPITAL':'The user is removed from the hospital',
                    'ASSIGN_USER_TO_HOSPITAL':'The user is assigned to the hospital',
                    'USER_PRIVILAGES':'The user is given privilages for this hospital',
                    'ADD_HOSPITAL_NODE':'New center has been added',
                    'EDIT_HOSPITAL_NODE':'Center information has been modified',
                    'DELETE_HOSPITAL_NODE':'Center has been deleted',
                    'ENABLE_HOSPITAL_NODE':'Center has been enabled',
                    'DISABLE_HOSPITAL_NODE':'Center has been disabled',
                    'TATPROVISION':'TAT has been provision',
                    'WORKLIST_RULE':'worklist rule has been provisioned',
                    'USER_LEAVE':'Leave has been provided',
                    'USER_LEAVE_DELETE':'Leave has been Canceled',
                    'WORKLIST_RULE_DELETE':'Worklist Rule Deleted',
                    'ADD_OR_UPDATE_MACRO':'Macro Updated',
                    'DELETE_MACRO':'Macro Deleted',
                    'STUDY_MARKED_NOT_REPORTED':'Study marked for not reporting',
                    'STUDY_MARKED_REPORTED': 'Study marked for reporting',
                    'ADD_TEMPLATE_NODE':'Adding template node',
                    'DELETE_TEMPLATE_NODE':'Template node Deleted',
                    'ADD_OR_UPDATE_TEMPLATE':'Template added/updated',
                    'DELETE_TEMPLATE':'Template deleted',
                    'ADD_OR_UPDATE_RAD_BILLING_CODE':'Add or Update radilogist billing code',
                    'ENABLE_DISABLE_RAD_BILLING_CODE':'Enable or Disable radiologist billing code',
                    'SAVE_RAD_MAPPED_TEMPLATE':'Save Radiologist mapped template',
                    'ENABLE_DISABLE_SAVE_RAD_MAPPED_TEMPLATE':'Enable or Disable Radiologist mapped template',
                    'ADDITIONAL_ACC_NO':'New accession no added to a study',
                    'SET_EMERGENCY':'Study marked as emergency',
                    'BROADCASTING_MESSAGE':'Send broadcasting message'
                    }
centerId=""
sessionId=""
def auth():
    run=True
    while (run) :
        if EntrprsCntr==1 and EnterpriseConnected=="True":
            enterpriseDbObj=model.session.query(model.RadArchivalConfig).first()
            hospitalDbObj=model.session.query(model.RadCenter).filter(model.RadCenter.id==0).first()
            parameters = {'login':hospitalDbObj.dicom_login,'passwd':hospitalDbObj.dicom_password,'source':"InstaMessageSync"}
            bot = HttpBot()
            result=bot.POST("https://"+ enterpriseDbObj.serverName+":"+enterpriseDbObj.Port+"/admin/do_DSCauth",parameters)
            log.debug("Reponse from central server %s",str(result))
            responseDictFromXML = xmltodict.parse(result)
            global centerId
            centerId=responseDictFromXML['Response']['ID']
            global sessionId
            sessionId=responseDictFromXML['Response']['Session']
            if str(centerId) =='-1':
                status="FAIL"
                log.error("DICOM username password doesn't correct")
            elif str(centerId) =='-2':
                status="FAIL"
                log.error("Centeral server RVP status is non zero")
            else:
                status="OK"
        
        else:
            log.info("Either number of Center is not equal to one or  ConnectivityToCentralServer  tag in webappconfig xml is false.")
            
        if status == "OK":
            run=False
        else:
            time.sleep(1000)

            
            

def postprocessor(path, key, value):
    #print path,":",key,":",value
    value= "" if value is None else value
    return key, value

def formatStudyMesageUsingStudyInstanceUID(studyInstanceUID):
    studyObj = model.session.query(model.RadStudy).filter(model.RadStudy.instanceUID==studyInstanceUID).first()
    if studyObj:
        nbOfImages=0
        seriesList = studyObj.series
        for series in seriesList:
            nbOfImages=nbOfImages+series.numOfBuffers
        return  studyObj.patient.firstName+" "+studyObj.patient.middleName+" "+studyObj.patient.lastName+", "+studyObj.patient.patientId+"<br/>"+studyObj.modality+", "+bodyPart+", "+nbOfImages+"<br/>"+studyObj.accessionNo

def put(eventType,dataDict,keyword={}):
    log.info("-------------------%s",dataDict)
    log.info("------------EventType is %s",eventType)
    try:
        dataDict['requestType']=eventType
        dataDict={'root':dataDict}
        
        #calculating message of the event
        StudyID = ''
        message = ""
        if eventType=='ADD_OR_UPDATE_USER':
            login = dataDict['root']['login']
            message = "Center admin has added/updated the user, login id is:- "+login
        
        elif eventType=='ENABLE_DISABLE_USER':
            login = dataDict['root']['login']
            enable = dataDict['root']['enable']
            log.info("enabled is %s",enable)
            if enable == True:
                message = "Admin has Enabled for this login: "+login
            else:
                message ="Admin has Disabled for this login: "+login
        
        elif eventType == 'CHANGE_PASSWORD':
            login = dataDict['root']['login']
            message = "Admin has changed the password for this user: "+login
        
        elif eventType=='ADD_OR_UPDATE_MACRO':
            if 'allmacro' in dataDict['root']:
                message = "Admin has request to resync all macros to all hospitals"
            else:
                message = "Admin has add or update marcos: "+dataDict['root']['macro']
            
        elif eventType=='DELETE_MACRO':
            message = "Admin has delete marco: "+dataDict['root']['macro']
         
        elif eventType=="ADD_TEMPLATE_NODE":
            if 'alltemplatenode' in dataDict['root']:
                message = "Admin has request to resync all template node to all hospitals"
            else:
                message = "Admin has added a template node: "+dataDict['root']['nodeName']
        
        elif eventType=="DELETE_TEMPLATE_NODE":
            message = "Admin has deleted a template node: "+dataDict['root']['nodeName']
        
        elif eventType=="ADD_OR_UPDATE_TEMPLATE":
            if 'alltemplate' in dataDict['root']:
                message = "Admin has request to resync all template  to allhospitals"
            else:
                message = "Admin has add/updated  a template: "+dataDict['root']['name']
        
        elif eventType=="DELETE_TEMPLATE":
            message = "Admin has deleted  a template  for hospital: "+dataDict['root']['templateName']
        
        elif eventType=="BROADCASTING_MESSAGE":
            message="New Broadcasting message published: "+dataDict['root']['message']
        
        elif eventType=="ADD_OR_UPDATE_RAD_BILLING_CODE":
            message = "Admin has added a radiologist billing code: "+dataDict['root']['RVUCode']
            
        elif eventType=="ENABLE_DISABLE_RAD_BILLING_CODE":
            message = "Admin has enable or disable a radiologist billing code: "+dataDict['root']['RVUCode']
        
        elif eventType=="ADD_OR_UPDATE_ITEM_BILLING_CODE":
            message = "Admin has added/updated an item code: "+dataDict['root']['billingcode']

        ###########################
        elif eventType=="ADD_OR_UPDATE_ITEM_MAPPED_TEMPLETE":
            message = "Admin has added/updated an item mapped to template for prcedure code: "+dataDict['root']['procedureCode']
        
        elif eventType=="DISABLE_ITEM_MAPPED_TEMPLETE":
            message = "Admin has disabled an item mapped to template for procedure code: "+dataDict['root']['procedureCode']
        
        elif eventType=='TATPROVISION':
            login = dataDict['root']['login']
            message = "TAT for following login "+login +" has been updated"
        
        elif eventType=="ADD_SESSION":
            login = dataDict['root']['login']
            message= login+" has login into central server"
        
        elif eventType=="ADDITIONAL_ACC_NO":
            message = "Accession no" + dataDict['root']['additionalAccNo'] + "added to study with accession no" + dataDict['root']['accessionNo']
        
        elif eventType=="REMOVE_SESSION":
            login = dataDict['root']['login']
            message= login+" has signout from central server"
        
        elif eventType=="MARK_CRITICAL_FINDING":
            
            if 'critical_case_instanceUID' in dataDict['root']:
                criticalCase=dataDict['root']['critical_case_instanceUID']
                StudyID = criticalCase
                query=model.session.query(model.RadStudy).filter(model.RadStudy.instanceUID==StudyID).first()
                modality=query.modality
                bodyPart=query.studyDesc
                accessionNo=query.accessionNo
                center_Name=query.studyCenter.name
                patient_name=query.patient.firstName+" "+query.patient.middleName+" "+query.patient.lastName
                patientID=query.patient.patientId
                message=modality+", "+bodyPart+"("+accessionNo+") of "+center_Name+" has been marked critical finding and assign to refer physicians. \r\n Patient: "+patient_name+", "+patientID

            else:
                criticalCase=dataDict['root']['studyInstanceUID']
                StudyID = criticalCase
                query=model.session.query(model.RadStudy).filter(model.RadStudy.instanceUID==StudyID).first()
                modality=query.modality
                bodyPart=query.studyDesc
                accessionNo=query.accessionNo
                center_Name=query.studyCenter.name
                patient_name=query.patient.firstName+" "+query.patient.middleName+" "+query.patient.lastName
                patientID=query.patient.patientId
                message=modality+", "+bodyPart+"("+accessionNo+") of "+center_Name+" has been marked critical finding . \r\n Patient: "+patient_name+", "+patientID
        
        elif eventType=="SET_EMERGENCY":
            StudyID = dataDict['root']['instanceUID']
            query=model.session.query(model.RadStudy).filter(model.RadStudy.instanceUID==StudyID).first()
            modality=query.modality
            bodyPart=query.studyDesc
            accessionNo=query.accessionNo
            center_Name=query.studyCenter.name
            patient_name=query.patient.firstName+" "+query.patient.middleName+" "+query.patient.lastName
            patientID=query.patient.patientId
            message=modality+", "+bodyPart+"("+accessionNo+") of "+center_Name+" has been marked emergency. \r\n Patient: "+patient_name+", "+patientID
        
        elif eventType=="PHYSICIAN_COMMENT":
            instanceUID = dataDict['root']['studyId']
            query=model.session.query(model.RadStudy).filter(model.RadStudy.instanceUID==instanceUID).first()
            modality=query.modality
            bodyPart=query.studyDesc
            accessionNo=query.accessionNo
            center_Name=query.studyCenter.name
            patient_name=query.patient.firstName+" "+query.patient.middleName+" "+query.patient.lastName
            patientID=query.patient.patientId
            message=modality+", "+bodyPart+"("+accessionNo+") of "+center_Name+" physician comment has been successfully done. \r\n Patient: "+patient_name+", "+patientID
        
        elif eventType=="STUDY_ASSIGNED_NEW":
            StudyID = dataDict['root']['studyInstanceUID']
            modality=dataDict['root']['Modality']
            bodyPart=dataDict['root']['bodyPart']
            accessionNo=dataDict['root']['accessionNo']
            center_Name=dataDict['root']['center_name']
            patient_name=dataDict['root']['patient_name']
            patientID=dataDict['root']['patientID']
            message=modality+", "+bodyPart+"("+accessionNo+") of "+center_Name+" has been assign to "+dataDict['root']['userLogin']+"\r\n Patient: "+patient_name+", "+patientID
        
        elif eventType=="REPORT_INVALIDATE":
            StudyID=dataDict['root']['studyid']
            query=model.session.query(model.RadStudy).filter(model.RadStudy.instanceUID==StudyID).first()
            modality=query.modality
            bodyPart=query.studyDesc
            accessionNo=query.accessionNo
            center_Name=query.studyCenter.name
            patient_name=query.patient.firstName+" "+query.patient.middleName+" "+query.patient.lastName
            patientID=query.patient.patientId
            message=modality+", "+bodyPart+"("+accessionNo+") of "+center_Name+" report has been invalidated. \r\n Patient: "+patient_name+", "+patientID
        
        elif eventType=="DICOM_EDIT":
            StudyID=dataDict['root']['studyid']
            query=model.session.query(model.RadStudy).filter(model.RadStudy.instanceUID==StudyID).first()
            modality=query.modality
            bodyPart=query.studyDesc
            accessionNo=query.accessionNo
            center_Name=query.studyCenter.name
            patient_name=query.patient.firstName+" "+query.patient.middleName+" "+query.patient.lastName
            patientID=query.patient.patientId
            message=modality+", "+bodyPart+"("+accessionNo+") of "+center_Name+" patient data has been edited. \r\n Patient: "+patient_name+", "+patientID
        
        elif eventType=="STUDY_UNLINK":
            StudyID=dataDict['root']['studyid']
            query=model.session.query(model.RadStudy).filter(model.RadStudy.instanceUID==StudyID).first()
            modality=query.modality
            bodyPart=query.studyDesc
            accessionNo=query.accessionNo
            center_Name=query.studyCenter.name
            patient_name=query.patient.firstName+" "+query.patient.middleName+" "+query.patient.lastName
            patientID=query.patient.patientId
            message=modality+", "+bodyPart+"("+accessionNo+") of "+center_Name+" request for unlink study. \r\n Patient: "+patient_name+", "+patientID
            if 'message' in dataDict['root']:
                message = dataDict['root']['message'] + message
        
        elif eventType=="NEW_STUDY" :
            StudyID=dataDict['root']['study']['instanceUID']
            patientName = dataDict['root']['patient']['firstName']
            patientID=dataDict['root']['patient']['patientId']
            modality=dataDict['root']['study']['modality']
            bodyPart=dataDict['root']['series'][0]['seriesDesc'] # only first series body part
            nbOfImage=dataDict['root']['nbOfImages']
            accessionNo=dataDict['root']['study']['accessionNo']
            if dataDict['root']['additionalstudy']=="true":
                message="Additional Images: "+patientName +", "+str(patientID)+"\r\n"+modality+", "+bodyPart+", "+str(nbOfImage)+"\r\n"+accessionNo
            else:
                message=patientName +", "+str(patientID)+"\r\n"+modality+", "+bodyPart+", "+str(nbOfImage)+"\r\n"+accessionNo          
        
        elif eventType=="ORDER_EDIT" :
            StudyID=dataDict['root']['study']['instanceUID']
            patientName = dataDict['root']['patient']['firstName']
            patientID=dataDict['root']['patient']['patientId']
            modality=dataDict['root']['study']['modality']
            bodyPart=dataDict['root']['series'][0]['seriesDesc'] # only first series body part
            nbOfImage=dataDict['root']['nbOfImages']
            accessionNo=dataDict['root']['study']['accessionNo']
            query=model.session.query(model.RadStudy).filter(model.RadStudy.instanceUID==StudyID).first()
            if EntrprsCntr>1:
                dataDict['root']['hospitalId']=query.studyCenter.id
                
            message=patientName +", "+str(patientID)+"\r\n"+accessionNo       
        
        elif  eventType=="SYNC_OLD_STUDY":
            patientName = dataDict['root']['patient']['firstName']
            patientID=dataDict['root']['patient']['patientId']
            modality=dataDict['root']['study']['modality']
            bodyPart=dataDict['root']['series'][0]['seriesDesc'] # only first series body part
            nbOfImage=dataDict['root']['nbOfImages']
            accessionNo=dataDict['root']['study']['accessionNo']
            if dataDict['root']['additionalstudy']=="true":
                message="Additional Images: "+patientName +", "+str(patientID)+"\r\n"+modality+", "+bodyPart+", "+str(nbOfImage)+"\r\n"+accessionNo
            else:
                message=patientName +", "+str(patientID)+"\r\n"+modality+", "+bodyPart+", "+str(nbOfImage)+"\r\n"+accessionNo   
        
        elif eventType=="STUDY_ASSIGNED_DELETED":
            StudyID = dataDict['root']['studyInstanceUID']
            modality=dataDict['root']['Modality']
            bodyPart=dataDict['root']['bodyPart']
            accessionNo=dataDict['root']['accessionNo']
            center_Name=dataDict['root']['center_name']
            patient_name=dataDict['root']['patient_name']
            patientID=dataDict['root']['patientID']
            message=modality+", "+bodyPart+"("+accessionNo+") of "+center_Name+" has been unassigned from "+dataDict['root']['userLogin']+"\r\n Patient: "+patient_name+", "+patientID
        
        elif eventType=="DELETE_STUDY":
            StudyID=dataDict['root']['studyID']
            #query=model.session.query(model.RadStudy).filter(model.RadStudy.instanceUID==StudyID).first()
            modality=dataDict['root']['modality']
            bodyPart=dataDict['root']['bodyPart']
            accessionNo=dataDict['root']['accessionNo']
            center_Name=dataDict['root']['center_Name']
            patient_name=dataDict['root']['patientName']
            patientID=dataDict['root']['patientId']
            message=modality+", "+bodyPart+"("+accessionNo+") of "+center_Name+" has been deleted. \r\n Patient: "+patient_name+", "+patientID
        
        elif eventType=="RTF_HEADER_FOOTER":
            centerID=dataDict['root']['hospitalId']           
            center_name=dataDict['root']['CenterName']
            vcenterId=dataDict['root']['vcenterId']
            TopMargin=dataDict['root']['topMargin']
            BottomMargin=dataDict['root']['bottomMargin']
            
            message="Header Footer for "+str(center_name)+" with top margin "+TopMargin+" ,bottom margin "+BottomMargin+" and vcenterId "+str(vcenterId)+" has been created. \r\n Center: "+str(center_name)
        
        elif eventType=="ADD_REPORT":
            StudyID=dataDict['root']['studyid']
            query=model.session.query(model.RadStudy).filter(model.RadStudy.instanceUID==StudyID).first()
            
            if EntrprsCntr>1:
                dataDict['root']['hospitalId']=query.studyCenter.id
                
            modality=query.modality
            bodyPart=query.studyDesc
            accessionNo=query.accessionNo
            center_Name=query.studyCenter.name
            patient_name=query.patient.firstName+" "+query.patient.middleName+" "+query.patient.lastName
            patientID=query.patient.patientId
            if query.reported=="S":
                report="SIGNEDOFF REPORT"
            elif query.reported=="D":
                report="DRAFTED REPORT"
            elif query.reported=="R":
                report="REVIEWED REPORT"
            else:
                report=""
            message=report+"\n Patient:"+patient_name+", "+patientID+"\n Scan:"+modality+", "+bodyPart+"("+accessionNo+") of "+center_Name+"\n Radiologist:"+dataDict['root']['physician']
        
        elif eventType=="DELETE_REPORT":
          
        
            StudyID=dataDict['root']['studyid']
            query=model.session.query(model.RadStudy).filter(model.RadStudy.instanceUID==StudyID).first()
            
            modality=query.modality
            bodyPart=query.studyDesc
            accessionNo=dataDict['root']['accessionNo']
            center_Name=query.studyCenter.name
            patient_name=query.patient.firstName+" "+query.patient.middleName+" "+query.patient.lastName
            patientID=query.patient.patientId
            if dataDict['root']['status']==3:
                report="SIGNEDOFF REPORT"
            elif dataDict['root']['status']==1:
                report="DRAFTED REPORT"
            elif dataDict['root']['status']==2:
                report="REVIEWED REPORT"
            else:
                report=""
            message=report+"\n Patient:"+patient_name+", "+patientID+"\n Scan:"+modality+", "+bodyPart+"("+accessionNo+") of "+center_Name+"\n Deleted By :"+dataDict['root']['login']
        
        elif eventType=="DELETE_MEDICAL_NOTES":
            
            orderNoObj = RISmodel.session.query(RISmodel.RadRISOrder).filter(RISmodel.RadRISOrder.dBId==dataDict['root']['orderId']).first()
            message= "Medical Note has delete for \n orderId:"+orderNoObj.orderNumber+"\n Patient:"+orderNoObj.patientDetailsForOrder.patientSearchableName+","+orderNoObj.patientDetailsForOrder.hospitalId
            
        elif eventType=="ADD_MEDICAL_NOTES":
            message= dataDict['root']['message']
        
        elif eventType=="STUDY_MERGE":
            sourceStudyID=dataDict['root']['sourceStudyInstanceUID']
            targetStudyID=dataDict['root']['targetStudyInstanceUID']
            StudyID = targetStudyID
            message= "STUDY_MERGE for source instanceUID: "+sourceStudyID+" and target instanceUID: "+targetStudyID
        
        elif eventType=="MARK_NOT_FOR_REPORTING":
            StudyID=dataDict['root']['instanceUID']
            query=model.session.query(model.RadStudy).filter(model.RadStudy.instanceUID==StudyID).first()
            if EntrprsCntr>1:
                dataDict['root']['hospitalId']=query.studyCenter.id
                
            if str(dataDict['root']['notForReporting']) == "1":
                message= "MARK NOT FOR REPORTING for instanceUID: "+StudyID
            else:
                message= "MARK FOR REPORTING for instanceUID: "+StudyID
        
        elif eventType=="PHYSICIAN_COMMENT":
            StudyID = dataDict['root']['studyId']
            query=model.session.query(model.RadStudy).filter(model.RadStudy.instanceUID==StudyID).first()
            modality=query.modality
            bodyPart=query.studyDesc
            accessionNo=query.accessionNo
            center_Name=query.studyCenter.name
            patient_name=query.patient.firstName+" "+query.patient.middleName+" "+query.patient.lastName
            patientID=query.patient.patientId
            message=modality+", "+bodyPart+"("+accessionNo+") of "+center_Name+" physician comment has been successfully done. \r\n Patient: "+patient_name+", "+patientID
        
        elif eventType=="SECOND_OPINION_COMMENTS":
            StudyID = dataDict['root']['studyInstanceUID']
            centeralizedReportId = dataDict['root']['centralizedReportId']
            query=model.session.query(model.RadStudy).filter(model.RadStudy.instanceUID==StudyID).first()
            if EntrprsCntr>1:
                dataDict['root']['hospitalId']=query.studyCenter.id
            message=" Second Opinion Comments has been successfully done for centeralizedReportId "+centeralizedReportId

        #End 
        
        #Calculating study id
        
        #if studyId!='':
        #    studyObj = model.session.query(model.RadStudy).selectfirst(model.RadStudy.instanceUID==kargs['studyId'])
        #    if studyObj !=None:
        #        uniqueID = studyObj.uniqueId
                
        #END 
        if "center" not in dataDict['root'].keys():
            hospitalDbObj=model.session.query(model.RadCenter).first()
            dataDict['root']['center']=util.convertObjectToDict1(hospitalDbObj)
            log.info("dataDict to add center:%s",dataDict)
        log.info("dataDict to add center:%s",dataDict)
        keywords=message
        hospitalName=""
        log.info("dataDict %s",str(dataDict))
        if keyword=="":
            keywords = keywords+"['"+keyword['patName']+"']"+"['"+keyword['patId']+"']"+"['"+keyword['accessionNo']+"']"
        #xml = xml2Dict_Dict2xml.ConvertDictToXmlString(dataDict)
        #log.info("Syncing %s",xml)
        #jsonData=simplejson.dumps(xml2Dict_Dict2xml.ConvertXmlStringToDict(xml)['root'])
        #jsonData=simplejson.dumps(xmltodict.parse(xml,postprocessor=postprocessor)['root'])
        jsonData=simplejson.dumps(util.convertBoolToString(dataDict['root']))
        log.info("Dumping JSON data:%s",jsonData)
        dbObj=None
        
        
        if eventType in ('NEW_STUDY','ADD_REPORT','SYNC_OLD_STUDY','ORDER_EDIT','ADD_MEDICAL_NOTES'):
            log.info("JSON  dataDict:%s",dataDict)
            jsonData1=jsonData
            jsonData={}
            
        if "hospitalId" not in dataDict['root'].keys():
            dataDict['root']['hospitalId']="-1"
        
        max_QueueId=1
        if model.session.query(model.GlobalXMLMessageQueueArchive).count()!=0:
            maxreportStatusQuery = model.session.query(SQLOperators.func.max(model.GlobalXMLMessageQueueArchive.messageQueueId).label("max_id"))
            res = maxreportStatusQuery.one()
            max_QueueId = int(res.max_id)
        max_QueueId = max_QueueId + 1
        log.info("max_QueueId--->%s",max_QueueId)
        if dataDict['root']['hospitalId']=="-1":
            log.info("adding message to sent to enterprise server.")
            enterpriseDbObj=model.session.query(model.RadArchivalConfig).first()
            if enterpriseDbObj != None:
                hospitalName="Center"
                dbObj=model.GlobalXMLMessageQueue(jsonData,StudyID,eventType,"Enterprise",enterpriseDbObj.serverName,enterpriseDbObj.Port,'NOT_PUSHED',keywords,-1,datetime.datetime.now(),'https',max_QueueId)
                model.session.add(dbObj)
        else:
            log.info("adding message to be sent local servers")
            hospitalDbObj=model.session.query(model.RadCenter).filter(model.RadCenter.id==dataDict['root']['hospitalId']).first()
            if hospitalDbObj != None:
                hospitalName=hospitalDbObj.name
                dbObj= model.GlobalXMLMessageQueue(jsonData,StudyID,eventType,hospitalDbObj.name,hospitalDbObj.centerIP,hospitalDbObj.centerPort,'NOT_PUSHED',keywords,dataDict['root']['hospitalId'],datetime.datetime.now(),hospitalDbObj.webProtocol,max_QueueId)
                model.session.add(dbObj)
        model.session.flush()
        
        if eventType in ('NEW_STUDY','ADD_REPORT','SYNC_OLD_STUDY','ORDER_EDIT','ADD_MEDICAL_NOTES'):
            tempPath = os.path.join(MessagequeueDocumentPath)
            try:
                os.stat(tempPath)
            except:
                os.mkdir(tempPath)
            try :
                log.info("print id ->%s",dbObj.id)
                tempPath = os.path.join(MessagequeueDocumentPath,str(dbObj.id))
                log.info("PRInt path : %s",tempPath)
                f = open(tempPath, 'wb')
                f.write(jsonData1)
                f.close()
            except Exception,e:
                log.info("Error while writing newstudy or add report--> %s",e)
            
        #    
        #uniqueID = ''
        #
        #
        #log.info("hospitalName%s",hospitalName)
        #logObj=model.RadTransmissionMessageLog(messageQueueId=dbObj.id,
        #                                        Message=message,
        #                                        studyId=studyId,
        #                                        uniqueID=uniqueID,                                                   
        #                                        eventType=eventType,
        #                                        status='IN_PROGRESS',
        #                                        creationDateTime=datetime.datetime.now(),
        #                                        lastUpdatedTime=datetime.datetime.now(),
        #                                        hospitalName=hospitalName)
        #
        #model.session.add(logObj)
        #model.session.flush()            
        
       
    except Exception,e:
        log.exception("ERROR:%s",e)
    
    
    return True
    
#
#class Singleton(object):
#    shouldTerminate=False
#    def __init__(self):
#        self.thread =None
#        
#singleton = Singleton()


def processingThread(destIp,destPort,destName):
    notPushedMessages=None
    pushedMessages=None
    log.info("Starting message syncing thread for:%s,%s",destName,destIp)
    while True:
        # expunge messagequeu row DB objects from memory, so that next time when we queuery on the same message we should get latest information.
        # i.e. information updated by other threads/applications.
        log.debug("%s: notPushedMessages %s",destName,notPushedMessages)
        if type(notPushedMessages)==type([]):
            for eachNotPushed in notPushedMessages:
                model.session.expire(eachNotPushed)
        #if type(pushedMessages)==type([]):
        #    for eachPushedMessage in pushedMessages:
        #        model.session.expire(eachPushedMessage)
        time.sleep(messagePollTime)
        log.info("%s,%s: MESSAGE SYNCING THREAD IS WORKING....",destName,destIp)
#            deleting pushed messages from DB.
        pushedMessages=model.session.query(model.GlobalXMLMessageQueue).filter(SQLOperators.and_(model.GlobalXMLMessageQueue.destIp==destIp,
                                                                                    model.GlobalXMLMessageQueue.status=="PUSHED")
                                                                        ).all()
        log.info("%s: Going to delete all pushed message",destName)
        for message in pushedMessages:
            tempPath = os.path.join(MessagequeueDocumentPath,str(message.id))
            log.debug("path : %s",tempPath)
            try:
                os.remove(tempPath)
                log.info("file deleted from folder %s",tempPath)
            except Exception,e:
                log.error("could not remove file: %s and Reason is: %s " % (tempPath,e))
            model.session.delete(message)
            model.session.flush()

#           read not pushed messages from DB.
        notPushedMessages=model.session.query(model.GlobalXMLMessageQueue).filter(SQLOperators.and_(model.GlobalXMLMessageQueue.destIp==destIp,
                                                                                    model.GlobalXMLMessageQueue.status=="NOT_PUSHED")
                                                                        ).all()
        bypassList=[]
        log.info("%s: Number of messages found to sync %s",destName,len(notPushedMessages))
        for i,message in enumerate(notPushedMessages):
            isSentFailure=0
            hospitalName=message.destName
            hospitalIP=message.destIp
            hospitalPort=message.destPort
            webProtocol=message.webProtocol
            message.lastUpdateDateTime=datetime.datetime.now()
            try:
                log.critical("%s: JSON Data:%s",destName,message.xmlMessage)
                
                if message.eventType in ('NEW_STUDY','ADD_REPORT',"SYNC_OLD_STUDY",'ORDER_EDIT','ADD_MEDICAL_NOTES'):
                    try:
                        tempPath = os.path.join(MessagequeueDocumentPath,str(message.id))
                        fd=open(tempPath,"rb+")
                        tempdata=fd.read()
                        size=len(tempdata)
                        fd.close()
                      
                        parameters = {'jsonData': json.dumps(json.loads(tempdata), separators=(',',':')),'sessionId':sessionId}
                    except Exception,e:
                        log.info("Error in reading paramater->%s",e)
                        message.status="ERROR"
                        model.session.flush()
                        continue
                else:
                    parameters = {'jsonData': message.xmlMessage,'sessionId':sessionId}
                    
                
                
                if hospitalIP not in bypassList:
                    bot = HttpBot()
                    log.info("%s: [%s/%s] Syncing message:%s to:%s ",destName,i+1,len(notPushedMessages),message.eventType,hospitalName)
                    if EntrprsCntr>1:
                        url=webProtocol+"://"+hospitalIP+":"+hospitalPort+"/local/handleMessage",parameters
                        log.info("********* Sending request from Center to local server**********")
                        log.info("-------url syncing to the  local/center servers------ %s",url)   
                        bot.POST(webProtocol+"://"+hospitalIP+":"+hospitalPort+"/local/handleMessage",parameters)
                    else:
                        url=webProtocol+"://"+hospitalIP+":"+hospitalPort+"/enterprise/handleMessage",parameters
                        log.info("********** Sending request from local to center ************* ")
                        log.info("-------url syncing to the  local/center servers------ %s",url)
                        result=bot.POST(webProtocol+"://"+hospitalIP+":"+hospitalPort+"/enterprise/handleMessage",parameters)
                        if result =='-1':
                            log.error("Session Id not found in central Server.")
                            isSentFailure=1
                            auth()
                        
                        elif result=="False":
                                log.error("Failed to handle fucntion in centeral server")
                                isSentFailure=1
                            
                        elif result == "UNKNOWN_MESSAGE_TYPE":
                            log.critical("Message type:%s has not been implemented.",message.eventType)
                            isSentFailure=1    
                            
                     
                    
                else:
                    log.critical("%s: [%s/%s] Not Syncing message:%s to:%s as Hospital Ip %s is in bypaas list may be server is down so not going for it.",destName,i,len(notPushedMessages),message.eventType,hospitalName,hospitalIP)
                    isSentFailure=1
            except Exception,e:
                log.critical("%s: Exception in message syncing==%s",destName,e)
                log.info('Updating satus of the message from In_Progress to Failed for msg :%s',message.id)
                #TransmissionLogObj=model.session.query(model.RadTransmissionMessageLog).filter(model.RadTransmissionMessageLog.messageQueueId==message.id).first()
                #if TransmissionLogObj != None:
                #    TransmissionLogObj.status='ERROR'
                #    #model.session.update(TransmissionLogObj)
                #    #model.session.flush()
                #else:
                #    log.info("No records found for this message Queue ID")
                ###model.session.delete(message)
                ##model.session.flush()

                err = sys.exc_info()
                sio = StringIO.StringIO()
                excep=StringIO.StringIO()
                hook = cgitb.Hook(file=sio)
                hook.handle(info=err)
                e1="<urlopen error (10061, 'Connection refused')>"
                e2="<urlopen error timed out>"
                if e1==str(e) or e2==str(e):
                    log.critical("%s: Adding Ip %s in bypass list. As it is an network Error",destName,hospitalIP)
                    #<sid temporary change>bypassList.append(hospitalIP)
                    isSentFailure=1
                else:
                    log.info("%s: It is python error. So after sleep of 2 sec will again sync",destName)
                    time.sleep(2)
                    try:
                        bot = HttpBot()
                        log.info("%s: [%s/%s] Syncing message:%s to:%s ",destName,i+1,len(notPushedMessages),message.eventType,hospitalName)
                        if EntrprsCntr>1:
                            url=webProtocol+"://"+hospitalIP+":"+str(hospitalPort)+"/local/handleMessage",parameters
                            log.info("********* Sending request from Center to local server**********")
                            log.info("-------url syncing to the  local/center servers------ %s",url)
                            bot.POST(webProtocol+"://"+hospitalIP+":"+hospitalPort+"/local/handleMessage",parameters)
                        else:
                            url=webProtocol+"://"+hospitalIP+":"+hospitalPort+"/enterprise/handleMessage",parameters
                            log.info("********** Sending request from local to center ************* ")
                            log.info("-------url syncing to the  local/center servers------ %s",url)
                            result=bot.POST(webProtocol+"://"+hospitalIP+":"+hospitalPort+"/enterprise/handleMessage",parameters)
                            if result =='-1':
                                log.error("Session Id not found in central Server.")
                                isSentFailure=1
                                auth()
                                
                            elif result=="False":
                                log.error("Failed to handle fucntion in centeral server")
                                isSentFailure=1
                            
                            elif result == "UNKNOWN_MESSAGE_TYPE":
                                log.critical("Message type:%s has not been implemented.",message.eventType)
                                isSentFailure=1
                            
                    except Exception,error:
                        err = sys.exc_info()
                        sio = StringIO.StringIO()
                        excep=StringIO.StringIO()
                        hook = cgitb.Hook(file=sio)
                        hook.handle(info=err)
                        log.critical("%s: Adding Ip %s in bypass list. As it is an network Error",destName)
                        isSentFailure=1
                traceback.print_exc(limit=None,file=excep)
                trace_plain=excep.getvalue()
                log.critical("%s: Error:%s",trace_plain,destName)
            else:
                if isSentFailure==0:
                    # abhishek: think of a better way to do this.
                    # currently, report upload is handled by RAU. Which check for those studies where updload has been done i.e. traked
                    # by column 'transactionUploadStatus' set to '1' in t_study.
                    
                    # better way can be using the message queue itself to push message of report creation/deletion/updation from local to central
                    # same way report can be pushed from central to local.
                    if message.eventType == "NEW_STUDY" or message.eventType == "SYNC_OLD_STUDY":
                        # update transactionUploadStatus to '1' in t_study.
                        log.debug("Updating column:t_study.transactionUploadStatus to '1'.")
                        data = simplejson.loads(tempdata)
                        radstudy = model.session.query(model.RadStudy).filter(model.RadStudy.instanceUID==data['study']['instanceUID']).first()
                        if radstudy:
                            radstudy.transactionUploadStatus = 1
                            model.session.flush()
                    
                    if message.eventType=="STUDY_MERGE":
                        log.debug("Updating column:t_backup_job.ModeUpdateDateTime to current datetime for study instanceUID (%s).",message.studyId)
                        NbofImages=0
                        verificationFailedBackupJobsObj = model.session.query(model.RadBackupJob).filter(SQLOperators.and_(model.RadBackupJob.studyInstanceUID==message.studyId,
                                                                        model.RadBackupJob.type=='BACKUP')).first()
                        if verificationFailedBackupJobsObj != None:
                            RadSeriesObj=model.session.query(model.RadSeries).filter(model.RadSeries.study_instanceUID==message.studyId)
                            for RadSeriesEntry in RadSeriesObj.all():
                                RadImageObj=model.session.query(model.RadImage).filter(model.RadImage.series_instanceUID==RadSeriesEntry.instanceUID)
                                for RadImageEntry in  RadImageObj.all():
                                    NbofImages=NbofImages+1
                                    verificationFailedBackupJobsDetailsObj = model.session.query(model.RadBackupJobDetails).filter(model.RadBackupJobDetails.sopInstanceUID==RadImageEntry.sopInstanceUID).first()
                                    if verificationFailedBackupJobsDetailsObj is None:
                                        backupJobDetails = model.RadBackupJobDetails(verificationFailedBackupJobsObj.dBId, RadSeriesEntry.instanceUID, RadImageEntry.sopInstanceUID)
                                        model.session.add(backupJobDetails)
                                        model.session.flush()
                            
                            if verificationFailedBackupJobsObj.status!='NOT_STARTED' :
                                verificationFailedRadArchivalLogsObj = model.session.query(model.RadArchivalLogs).filter(SQLOperators.and_(model.RadArchivalLogs.studyInstanceUID==message.studyId,
                                                                                            model.RadArchivalLogs.jobtype=='UPLOAD')).first()
                                verificationFailedRadArchivalLogsObj.transcation_start_time=datetime.datetime.now()
                                verificationFailedRadArchivalLogsObj.transcation_end_time=None
                                verificationFailedRadArchivalLogsObj.queue_start_time=None
                                verificationFailedRadArchivalLogsObj.status='NOT_STARTED'
                                verificationFailedRadArchivalLogsObj.retrieval_orginator=''
                                verificationFailedRadArchivalLogsObj.archival_xml_response=''
                                verificationFailedRadArchivalLogsObj.ModeUpdateDateTime=None
                                verificationFailedRadArchivalLogsObj.ModeChangedBy=''
                                verificationFailedRadArchivalLogsObj.NbOfImages=NbofImages
                            
                                verificationFailedBackupJobsObj.status="NOT_STARTED"
                                verificationFailedBackupJobsObj.device="DATA_CENTER"
                                verificationFailedBackupJobsObj.type="BACKUP"
                                model.session.flush()
                        

                   
                            
                            
                        log.info("Successfully updated column ModeUpdateDateTime in t_backup_jobs for target instanceUID--->(%s)",message.studyId)
                                #---------Going to sync meta data for source instanceUID.------
                        log.info("Going to sync meta data for source instanceUID--->(%s)",message.studyId)
                        try:
                            util.send_new_study_to_central_server(message.studyId)
                        except Exception,e:
                            log.info("exception is %s",e)
                    
                    message.status="PUSHED"
                    #model.session.update(message)
                    log.info("%s: [%s/%s]Successfully synced message:%s to:%s",destName,i+1,len(notPushedMessages),message.eventType,hospitalName)
                model.session.flush()
                
def start():
    log.info("Starting processing")
    if EntrprsCntr> 1:
        hospitalObj = model.session.query(model.RadCenter).all()
        for hospital in hospitalObj:
            thread=threading.Thread(target=processingThread,kwargs={"destIp":hospital.centerIP,"destPort":hospital.centerPort,"destName":hospital.name})
            thread.setDaemon(True)
            thread.start()
    else:
        auth()
        hospitalObj=model.session.query(model.GlobalConfiguration).all()
        for hospital in hospitalObj:
            name="center"
            thread=threading.Thread(target=processingThread,kwargs={"destIp":hospital.globalServerIP,"destPort":hospital.globalServerPort,"destName":name})
            thread.setDaemon(True)
            thread.start()
    