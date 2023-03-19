#fetch data from NIWA virtual station network
import sys
import os

import math

import datetime as dt
import dateparser as dp
from io import StringIO

import json
import jsonschema
from jsonschema import validate

import requests as req
from requests.auth import HTTPBasicAuth
import requests_cache as reqC

import numpy as np
import pandas as pd

class kFetchVCSN:
    #debugging flag
    _debugChange = False
    
    #all non exposed static variables go here
    ##data mashup from NIWA github https://github.com/niwa/data-mashup/blob/master/python_script.py
    _apiRoot = None
    _initialised = False
    
    #tables with static information
    __measTable = pd.DataFrame()
    __gridTable = pd.DataFrame()
    
    #credentials, to be initialised
    _uName = None
    _uPswd = None
    
    #operating with the site
    _agentNo = None
    
    def __init__(self,apiUrl:str='',refreshInterval:'sec'=3*60*60,enableDebug:'boolean' = False):
        #this is the key to let other functions know if we need debugging info
        self.debug = enableDebug

        #the caching reduces the number of hits to the origin server
        if refreshInterval > 0:
            reqC.install_cache('VCSN_cache', backend='sqlite', expire_after=refreshInterval)

        self._apiRoot = (
            apiUrl
            or "https://mintaka.niwa.co.nz/rest/api/V1.1/products/geo/data/1"
        )
        #load the default files
        self._loadGridLocation()
        self._loadMeasurementTypes()
            
    #-----------------------------------------------------------------------
    #private functions
    #-----------------------------------------------------------------------
    def _loadGridLocation(self):
        #this is based on file at the moment, however, there might be a better way
        #the file is sourced from Jeff Cooke and Kathleen
        self.__gridTable = pd.read_csv('VCSN_SitesExample.csv')
        
    def _loadMeasurementTypes(self):
        #this is based on file at the moment, however, there might be a better way
        #the file is sourced from Jeff Cooke and Kathleen
        try:
            self.__measTable = pd.read_csv('VCSNRequiredmeasurements_all.csv')
        except Exception as er:
            self.__measTable = pd.read_csv('VCSNRequiredmeasurements_all.csv', encoding='mbcs', engine='python')
        
    def __get_measurementId(self, measurement):
        if isinstance(measurement,str):
            return self.__measTable[self.__measTable['propName'] == measurement]['PRODUCTID'].values[0]
        else:
            raise Exception('please refer to a proper measurement code that NIWA provides on web portal.')
    
    def __get_nearest_agentNo(self, pos:('lat','lon'), searchRadius:'deg len' = 0.05):
        if isinstance(pos,tuple):
            redDf = self.__gridTable[(self.__gridTable['LONGT'].between(pos[1]-searchRadius, pos[1]+searchRadius))
            & (self.__gridTable['LAT'].between(pos[0]-searchRadius, pos[0]+searchRadius))].copy()
            #calc nearest and sort by distance
            redDf['dist'] = redDf.apply(lambda row:
                                          math.sqrt((float(pos[0]) - float(row.LAT))**2 
                                          + (float(pos[1]) - float(row.LONGT))**2)
                                          , axis=1)
            redDf.sort_values(by=['dist'],inplace=True)
            redDf.reset_index(drop=True, inplace=True)
            return redDf['AGENT_NO'][0]
        else:
            print('variable type issue')
            return None
    
    def __get_timeSpan(self,site,measurement):
        if isinstance(measurement,str):
            measurement = self.__get_measurementId(measurement)
        elif not isinstance(measurement, int) and not isinstance(
            measurement, np.int64
        ):
            raise Exception('please refer to a proper measurement code that NIWA provides on web portal.')

        if isinstance(site, (int, np.int64)):
            agentNo = site
        elif isinstance(site,tuple):
            agentNo = self.__get_agentNo(site)
        else:
            print(type(site))
            raise Exception('please refer to a proper agent no that NIWA provides on web portal.')

        myWebRequest = self._apiRoot+'/'+str(agentNo)+'/'+str(measurement)
        reply = self.__webFetch(myWebRequest)
        """
        schema = '{"type": "object", "properties": { "productClass": { "type": "string" }, "geoDomainId": { "type": "integer" }, "featureId": { "type": "integer" }, "startDate": { "type": "string" }, "endDate": { "type": "string" }, "data": { "type": "array", "items": [ { "type": "object", "properties": { "validityTime": { "type": "string" }, "value": { "type": "number" } }, "required": [ "validityTime", "value" ] } ] }, "productId": { "type": "integer" }, "externalProductId": { "type": "string" }, "name": { "type": "string" }, "description": { "type": "string" }, "dataSourceType": { "type": "string" } }, "required": [ "productClass", "geoDomainId", "featureId", "startDate", "endDate", "data", "productId", "externalProductId", "name", "description", "dataSourceType" ] }'
        #do sanity checks before returning r
        if self.__validateJson(reply,schema):
            pass
        else :
            raise Exception('issue with web reply',reply.text)
        """
        if isinstance(reply, tuple) and reply[0] is None:
            return None
        else:
            return [dp.parse(reply['startDate']),dp.parse(reply['endDate'])]
        
    def __validateJson(self,jsonData,schema):
        try:
            validate(instance=jsonData, schema=schema)
        except jsonschema.exceptions.ValidationError as err:
            return False
        return True
    
    def __webFetch(self,myWebRequest):
        if self._apiRoot is None:
            return None
            
        if myWebRequest != '' and isinstance(myWebRequest, str):
            if self.debug:
                print(myWebRequest)
                print('Start online transaction',sys._getframe(1).f_code.co_name)
            
            headers = {
                'Accept': "*/*",
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/95.0.4638.69 Safari/537.36'
            }
            
            #r = req.get(myWebRequest)
            #the following method can add params if required params=request_params if auth is needed
            s = req.Session()
            s.headers.update(headers)
            """
            if self._uName == None and self._uPswd == None:
                print('empty username and password fields')
                p = req.Request('GET', myWebRequest).prepare()
            else:

                #s.auth = (self._uName, self._uPswd)
                #hostname = self._apiRoot.split('/')[2]
                ##print(hostname)
                #auth = s.post('http://' + hostname)
                #auth = HTTPBasicAuth(self._uName, self._uPswd)
                p = req.Request('GET', myWebRequest, auth=(self._uName, self._uPswd)).prepare()
            """
            p = req.Request('GET', myWebRequest, auth=(self._uName, self._uPswd)).prepare()
            try:
                r = s.send(p)
                #r = req.get(myWebRequest, auth=(self._uName, self._uPswd), headers=headers)
                r.raise_for_status()
            except req.exceptions.Timeout:
                # Maybe set up for a retry, or continue in a retry loop
                print('request timed out')
                return None, 'erTO'
            except req.exceptions.ConnectionError as errc:
                print ("Error Connecting",errc)
                sys.exit(errc)
                #return None,'erCE'
            except req.exceptions.TooManyRedirects:
                # Tell the user their URL was bad and try a different one
                print('too many redirects')
                return None,None
            except req.exceptions.HTTPError as err:
                #raise SystemExit(err)
                print('unauthorised access', err)
                return None,None
            except req.exceptions.RequestException as err:
                # catastrophic error. bail.
                print(err)
                return None,None
            except Exception as e:
                print(e)
                return None, None
            self._debugChange = 'webFetch'

            if self.debug:
                print('End online transaction')
            
            #print(r.json(),'reply')
            return r.json()
        
    def __get_measName(self,measurement=None):
        if measurement is None:
            return None
        if isinstance(measurement,str):
            measurement = int(measurement)
        temp = self.__measTable[self.__measTable['PRODUCTID']==measurement]
        #print(temp)
        return temp['propName'].values[0]
    #---------------------------------------------------------------------
    #Public functions
    #---------------------------------------------------------------------
    
    #Functions to get the maintenance
    #---------------------------------------------------------------------
    def clobberCache(self) -> None:
        try:
            os.remove("VCSN_cache.sqlite")
        except OSError as e: 
            if e.errno != errno.ENOENT: # errno.ENOENT = no such file or directory
                raise # re-raise exception if a different error occurred
    
    def fetchData(self, measurement:'propName | PRODUCTID', startTime=None, endTime=None):
        if isinstance(measurement,str):
            measurement = self.__get_measurementId(measurement)
        elif isinstance(measurement,int) or isinstance(measurement,np.int64):
            pass
        else:
            raise Exception('please refer to a proper measurement code that NIWA provides on web portal.')
        
        if self._agentNo != None:
            agentNo = self._agentNo
        else:
            raise Exception ('please set the desired site')
        
        if startTime == None or endTime == None:
            [sTime,eTime] = self.__get_timeSpan(agentNo,measurement)
            if startTime == None:
                startTime = sTime
            elif isinstance(startTime,str):
                startTime = dt.datetime.strptime(startTime, "%Y-%m-%d")
            elif not isinstance(startTime,dt.date):
                raise Exception('start time should be', type(dt.date))
            if endTime == None:
                endTime = eTime
            elif isinstance(endTime,str):
                endTime = dt.datetime.strptime(endTime, "%Y-%m-%d")
            elif not isinstance(endTime,dt.date):
                raise Exception('end time should be', type(dt.date))
                
        startTime = str(startTime).replace('+00:00','Z').replace(' ','T')
        if pd.isnull(startTime):
            startTime = '1990-07-01T00:00:00'
            
        endTime = str(endTime).replace('+00:00','Z').replace(' ','T')
        if pd.isnull(endTime):
            endTime = str(datetime.datetime.now())
            
        agentNo = str(agentNo)
        measurement = str(measurement)
        myWebRequest = self._apiRoot+'/'+agentNo+'/'+measurement+'?startDate='+startTime+'&endDate='+endTime
        reply = self.__webFetch(myWebRequest)
        try:
            myDf = pd.DataFrame(data=reply['data'])
            myDf.rename(columns={'value':str(self.__get_measName(measurement=measurement))},inplace=True)
            return myDf
        except Exception as er:
            print('no data found')
            return None
    
    #credentials need to be set.
    def __setCredentials(self, arg):
        if not isinstance(arg, tuple):
            raise Exception ('expected (username,password), not', arg)
        (userName, passWord) = arg
        self._uName = userName
        self._uPswd = passWord
    def __getCreds(self):
        return None
        
    myCredentials = property(__getCreds, __setCredentials) #moved to init
    
    #fetching nearest station is exposed as public method
    def __setAgentNo(self, agentNo:'int | (lat,lon)'=None):
        if isinstance(agentNo,int):
            self._agentNo = agentNo
        elif isinstance(agentNo,tuple):
            pos = tuple(float(el) for el in agentNo) #this variable is overloaded
            if (-90<pos[0]<90) and (-180<pos[1]<360):
                self._agentNo = self.__get_nearest_agentNo(pos)
                print('selected ', self._agentNo)
                return self._agentNo
            else:
                raise Exception('site should have WGS84 (lat,lon) coordinates')
        else:
            raise Exception('please refer to a proper agent no that NIWA provides on web portal.')
                
    def __getAgentNo(self):
        return self._agentNo
        
    selectSite = property(__getAgentNo, __setAgentNo) #moved to init
        
    
    