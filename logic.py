#!/usr/bin/python3

import sys, os
# Add relative paths for the directory where the adapter is located as well as the parent
sys.path.append(os.path.dirname(__file__))
sys.path.append(os.path.join(os.path.dirname(__file__),'../../base'))

from sofabase import sofabase, adapterbase, configbase
import devices

from sofacollector import SofaCollector
from sendmail import mailSender
from concurrent.futures import ThreadPoolExecutor

import json
import asyncio
import concurrent.futures
import datetime
import time
import uuid
import aiohttp
import aiofiles
from aiohttp import web
import copy
from operator import itemgetter
from astral import LocationInfo
from astral.sun import sun

class logic(sofabase):

    class adapter_config(configbase):
    
        def adapter_fields(self):
            self.location=self.set_or_default('location', mandatory=True)
            self.automation_directory=self.set_or_default('automation_directory', mandatory=True)
            self.scene_directory=self.set_or_default('scene_directory', mandatory=True)
            self.area_directory=self.set_or_default('area_directory', mandatory=True)
            self.since=self.set_or_default('since', default={})
            self.fixes=self.set_or_default('fixes', default={})

    
    class EndpointHealth(devices.EndpointHealth):

        @property            
        def connectivity(self):
            return 'OK'

    class PowerController(devices.PowerController):
        
        @property            
        def powerState(self):
            return "ON" if self.nativeObject['active'] else "OFF"
            
        async def TurnOn(self, correlationToken='', **kwargs):
            try:
                if self.deviceid in self.adapter.modes:
                    changes=await self.adapter.dataset.ingest({ "mode" : { self.deviceid: {'active':True }}})
                    self.adapter.saveJSON('modes',self.adapter.modes)
                return await self.adapter.dataset.generateResponse(self.device.endpointId, correlationToken)
            except:
                self.log.error('!! Error during TurnOn', exc_info=True)
                return None

        async def TurnOff(self, correlationToken='', **kwargs):
            try:
                if self.deviceid in self.adapter.modes:
                    changes=await self.adapter.dataset.ingest({ "mode" : { self.deviceid: {'active':False }}})
                    self.adapter.saveJSON('modes',self.adapter.modes)
                return await self.adapter.dataset.generateResponse(self.device.endpointId, correlationToken)
            except:
                self.log.error('!! Error during TurnOff', exc_info=True)
                return None

    class LogicController(devices.LogicController):
        
        @property            
        def time(self):
            return datetime.datetime.now().time()

        @property            
        def sunrise(self):
            return sun(self.adapter.location.observer, date=datetime.datetime.now())['sunrise'].time()

        @property            
        def sunset(self):
            return sun(self.adapter.location.observer, date=datetime.datetime.now())['sunset'].time()


        async def Alert(self, correlationToken='', **kwargs):
            try:
                self.log.info('Sending Alert: %s' % payload['message']['text'])
                await self.adapter.runAlert(payload['message']['text'])
                response=await self.adapter.dataset.generateResponse(self.device.endpointId, correlationToken)
                return response
            except:
                self.log.error('!! Error during alert', exc_info=True)
                return None

        async def Delay(self, payload, correlationToken='', **kwargs):
            try:
                self.log.info('Delaying for %s seconds' % payload['duration'])
                await asyncio.sleep(int(payload['duration']))
                response=await self.adapter.dataset.generateResponse(self.device.endpointId, correlationToken)
                return response
            except:
                self.log.error('!! Error during alert', exc_info=True)
                return None

    class SceneController(devices.SceneController):
        
        async def Delete(self, correlationToken='', **kwargs):
            try:
                response=await self.adapter.deleteScene(self.device.endpointId)
                return response
            except:
                self.log.error('!! Error during delete', exc_info=True)
                return None

        async def Deactivate(self, correlationToken='', cookie={}):
            self.log.warn('!! Deactivate has not been implemented')

        async def Activate(self, correlationToken='', cookie={}):
            try:
                trigger={}
                if 'trigger' in cookie:
                    trigger=cookie['trigger']
                triggerEndPointId=''
                if 'triggerEndpointId' in cookie:
                    triggerEndPointId=cookie['triggerEndPointId']
                if self.deviceid in self.adapter.automations:
                    activation_started_report=self.ActivationStarted()
                    run_id="%s-%s" % (activation_started_report['event']['endpoint']['endpointId'], activation_started_report['event']['payload']['timestamp'])
                    if 'conditions' in cookie:
                        task = asyncio.create_task(self.adapter.runActivity(self.deviceid, trigger, triggerEndPointId, conditions=cookie['conditions'], run_id=run_id))
                    else:
                        task = asyncio.create_task(self.adapter.runActivity(self.deviceid, trigger, triggerEndPointId, run_id=run_id))
                    #self.log.info('Started automation as task: %s /%s' % (device, task))
                    activation_started_report=self.ActivationStarted()
                    return activation_started_report
                    # This should return the scene started ack
                elif self.deviceid in self.adapter.scenes:
                    await self.adapter.runScene(self.deviceid)
                else:
                    self.log.info('Could not find scene or activity: %s' % device)
                    return {}
                
                return self.device.Response(correlationToken)
                #response=await self.adapter.dataset.generateResponse(self.device.endpointId, correlationToken)
                #return response
            except:
                self.log.error('!! Error during activate', exc_info=True)
                return None


    class AreaController(devices.AreaController):

        @property            
        def children(self):
            return self.nativeObject['children']
            
        @property            
        def shortcuts(self):
            if 'shortcuts' in self.nativeObject:
                return self.nativeObject['shortcuts']
            return []

        @property            
        def scene(self):
            if 'scene' in self.nativeObject:
                return self.nativeObject['scene']
            else:
                return ""

        @property            
        def level(self):
            return 0

        async def Snapshot(self, payload, correlationToken='', **kwargs):
            try:
                self.log.info('Snapshotting Area as scene: %s %s' % (self.device.endpointId, payload))
                await self.adapter.captureSceneFromArea(self.device.endpointId, payload)
                return {}
            except:
                self.adapter.log.error('!! Error during Snapshot', exc_info=True)
                return None

        async def SetChildren(self, payload, correlationToken='', **kwargs):
            try:
                self.log.info('!! previous children of %s: %s' % (self.device.friendlyName, self.nativeObject['children']))
                self.log.info('!! new children %s' % (payload['children']))
                #newarea=dict(self.nativeObject)
                #newarea['children']=payload['children']['value']
                #await self.adapter.dataset.ingest({ 'area': { self.device.friendlyName : payload['children']['value'] }}, overwriteLevel='/%s/%s/children' % ('area', self.device.friendlyName))
                await self.adapter.dataset.ingest(payload['children']['value'], overwriteLevel='/%s/%s/children' % ('area', self.device.friendlyName))

                self.log.info('.. attempting to save %s %s' % (self.device.friendlyName, self.nativeObject))
                await self.adapter.save_data_to_directory('area', self.device.friendlyName, self.nativeObject)

                
            except:
                self.adapter.log.error('!! Error during SetChildren', exc_info=True)
        
        async def SetShortcuts(self, correlationToken='', **kwargs):
            try:
                self.log.info('!! SetShortcuts is not implemented')
            except:
                self.adapter.log.error('!! Error during SetShortcuts', exc_info=True)
        
        async def SetScene(self, correlationToken='', **kwargs):
            try:
                self.log.info('!! SetScene is not implemented')
            except:
                self.adapter.log.error('!! Error during SetScene', exc_info=True)
  
     
    class adapterProcess(SofaCollector.collectorAdapter):

        @property
        def collector_categories(self):
            return ['ALL']    

        def __init__(self, log=None, loop=None, dataset=None, notify=None, request=None, config=None, **kwargs):
            super().__init__(log=log, loop=loop, dataset=dataset, config=config)
            self.automations={}
            self.areas={}
            self.dataset.nativeDevices['scene']={}
            self.dataset.nativeDevices['activity']={}
            self.native_group_cache={}
            self.running_activities={}
            
            self.dataset.nativeDevices['logic']={}
            self.dataset.nativeDevices['mode']={}
            self.dataset.nativeDevices['area']={}
            self.area_calc_pending=[] # waiting for calc to finish
            self.area_calc_deferred=[] # waiting for adapter to be less busy
            self.since=self.config.since # TODO/CHEESE this list should be calcuated instead of config
            self.location=LocationInfo( self.config.location["city"], self.config.location["country"], 
                                        self.config.location["time_zone"],
                                        self.config.location['lat'], self.config.location['long'])
            
            self.logicpool = ThreadPoolExecutor(10)
            self.busy=True
            self.notify=notify
                

        async def virtualAdd(self, datapath, data):
            
            try:
                dp=datapath.split("/")
                if dp[0]=='automation':
                    result=await self.saveAutomation(dp[1], data)
                elif dp[0]=='scene':
                    result=await self.saveScene(dp[1], data)  
                elif dp[0]=='region':
                    result=await self.saveRegion(dp[1], data)  
                elif dp[0]=='area':
                    result=await self.saveArea(dp[1], data)  

                else:
                    return '{"status":"failed", "reason":"No Save Handler Available for type %s"}' % dp[0]
                    
                if result:
                    if dp[0]=='automation':
                        for auto in self.automations:
                            await self.dataset.ingest({"activity": { auto : self.automations[auto] }})
                    elif dp[0]=='mode':
                        for mode in self.modes:
                            await self.dataset.ingest({"mode": { mode : self.modes[mode] }})
                    elif dp[0]=='scene':    
                        for scene in self.scenes:
                            await self.dataset.ingest({"scene": { scene : self.scenes[scene] }})
                    return '{"status":"success", "reason":"Save Handler completed for %s"}' % datapath

                else:
                    return '{"status":"failed", "reason":"Save Handler did not complete for %s"}' % datapath
            
            except:
                self.log.error('Error loading pattern: %s' % jsonfilename,exc_info=True)
                return '{"status":"failed", "reason":"Internal Adapter Save Handler Error"}'

        async def virtualDel(self, datapath, data):
            
            try:
                self.log.info('.. logic virtual del %s %s' % (datapath, data))
                dp=datapath.split("/")
                if dp[0]=='automation':
                    result=await self.delAutomation(dp[1])
                elif dp[0]=='scene':
                    result=await self.delScene(dp[1])  
                elif dp[0]=='region':
                    result=await self.delRegion(dp[1])  
                elif dp[0]=='area':
                    result=await self.delRegion(dp[1])  

                else:
                    return '{"status":"failed", "reason":"No Del Handler Available for type %s"}' % dp[0]
                    
                if result:
                    return '{"status":"success", "reason":"Del Handler completed for %s"}' % datapath
                else:
                    return '{"status":"failed", "reason":"Del Handler did not complete for %s"}' % datapath
            
            except:
                self.log.error('Error loading pattern: %s' % jsonfilename,exc_info=True)
                return '{"status":"failed", "reason":"Internal Adapter Del Handler Error"}'

                
        async def virtualSave(self, datapath, data):
            
            try:
                dp=datapath.split("/")
                if dp[0]=='automation':
                    result=await self.saveAutomation(dp[1], data)
                elif dp[0]=='scene':
                    result=await self.saveScene(dp[1], data)  
                elif dp[0]=='region':
                    result=await self.saveRegion(dp[1], data)  
                elif dp[0]=='area':
                    result=await self.saveArea(dp[1], data)  

                else:
                    return '{"status":"failed", "reason":"No Save Handler Available for type %s"}' % dp[0]
                    
                if result:
                    return '{"status":"success", "reason":"Save Handler completed for %s"}' % datapath
                else:
                    return '{"status":"failed", "reason":"Save Handler did not complete for %s"}' % datapath
                
                    
            except:
                self.log.error('Error loading pattern: %s' % jsonfilename,exc_info=True)
                return '{"status":"failed", "reason":"Internal Adapter Save Handler Error"}'
            
        def jsonDateHandler(self, obj):

            if hasattr(obj, 'isoformat'):
                return obj.isoformat()
            else:
                self.log.error('Found unknown object for json dump: (%s) %s' % (type(obj),obj))
            return None

        async def saveAutomation(self,name,data):
            try:
                self.log.info('Saving Automation %s: %s' % (name, data))
                data=json.loads(data)
                if name not in self.automations:
                    self.automations[name]={"lastrun": "never", "triggers": [], "actions": [], "conditions": [], "schedules":[], "favorite": False }
                
                if 'name' not in data:
                    self.automations[name]['name']=name
                if 'filename' not in data:
                    self.automations[name]['filename']=name+".json"
                
                if 'actions' in data:
                    self.automations[name]['actions']=data['actions']
                if 'conditions' in data:
                    self.automations[name]['conditions']=data['conditions']
                if 'triggers' in data:
                    self.automations[name]['triggers']=data['triggers']
                if 'schedules' in data:
                    self.automations[name]['schedules']=data['schedules']
                    
                async with aiofiles.open(os.path.join(self.config.automation_directory, name+".json"), 'w') as f:
                    await f.write(json.dumps(self.automations[name]))

                if "logic:activity:%s" % name not in self.dataset.localDevices:
                    await self.dataset.ingest({"automations": { name : self.automations[name] }})
                #self.eventTriggers=self.buildTriggerList()
                return True

            except:
                self.log.error('Error saving automation: %s %s' % (name, data), exc_info=True)
                return False

        async def saveScene(self, name, data):
            try:
                try:
                    data=json.loads(data)
                except TypeError:
                    pass
                
                self.scenes[name]=data
                
                async with aiofiles.open(os.path.join(self.dataset.config.scene_directory, name+".json"), 'w') as f:
                    await f.write(json.dumps(data))
                
                if "logic:scene:%s" % name not in self.dataset.localDevices:
                    await self.dataset.ingest({"scene": { name : data }})
                return True
            except:
                self.log.error('Error saving automation: %s %s' % (name, data), exc_info=True)
                return False

        async def deleteScene(self, endpointId):
            
            try:
                for scene in self.scenes:
                    if self.scenes[scene]['endpointId']==endpointId:
                        self.log.info('.. Deleting scene %s (%s)' % (scene, endpointId))
                        del self.scenes[scene]
                        for dev in self.dataset.nativeDevices['scene']:
                            if self.dataset.nativeDevices['scene'][dev]['endpointId']==endpointId:
                                del self.dataset.nativeDevices['scene'][dev]
                                break
                        self.saveJSON('scenes',self.scenes)
                        delreport=await self.dataset.generateDeleteReport(endpointId)
                        self.dataset.deleteDevice(endpointId)
                        for area in self.areas:
                            if endpointId in self.areas[area]['children']:
                                newchildren=self.areas[area]['children'].copy()
                                newchildren.remove(endpointId)
                                await self.dataset.ingest(newchildren, overwriteLevel='/area/%s/children' % area)
                                self.log.info('.. Removed %s from %s' % (endpointId, area))
                        self.saveJSON('areas', self.areas)
                        break
                    
            except:
                self.log.error('Error deleting scene: %s' % endpointId, exc_info=True)

        async def delAutomation(self, name):
            
            try:
                if name in self.automations:
                    self.log.info('.! Deleting automation: %s' % name)
                    if os.path.exists(os.path.join(self.config.automation_directory,name+".json")):
                        os.remove(os.path.join(self.config.automation_directory,name+".json"))
                    del self.automations[name]
                self.calculateNextRun()
                return True
            except:
                self.log.error('Error deleting automation: %s' % name, exc_info=True)
                return False

        async def delRegion(self, name):
            
            try:
                if name in self.regions:
                    del self.regions[name]
                
                self.saveJSON('regions',self.regions)
                return True
            except:
                self.log.error('Error deleting automation: %s' % name, exc_info=True)
                return False

        async def delArea(self, name):
            
            try:
                if name in self.areas:
                    del self.areas[name]
                
                self.saveJSON('areas',self.areas)
                return True
            except:
                self.log.error('Error deleting automation: %s' % name, exc_info=True)
                return False

        async def saveArea(self, name, data):
            
            try:
                self.log.info('Saving Area: %s %s' % (name, data))
                if type(data)==str:
                    data=json.loads(data)
                    
                self.areas[name]=data

                self.saveJSON('areas',self.areas)

                return True
            except:
                self.log.error('Error saving area: %s %s' % (name, data), exc_info=True)
                return False

        async def saveRegion(self, name, data):
            
            try:
                if type(data)==str:
                    data=json.loads(data)
                    
                self.log.info('Saving Region: %s %s' % (name, data['areas'].keys()))
                    
                if 'areas' in data:
                    if type(data['areas'])!=list:
                        data={'areas': list(data['areas'].keys()) }
                    else:
                        data={ 'areas': data['areas'] }
                    
                    self.regions[name]=data

                    self.saveJSON('regions',self.regions)
                    return True
                else:
                    self.log.error('!~ No area data in region %s save: %s' % (name,data))
                    return False
                    
            except:
                self.log.error('Error saving region: %s %s' % (name, data), exc_info=True)
                return False


        def fixdate(self, datetext):
            try:
                working=datetext.split('.')[0].replace('Z','')
                if working.count(':')>1:
                    return datetime.datetime.strptime(working, '%Y-%m-%dT%H:%M:%S')    
                else:
                    return datetime.datetime.strptime(working, '%Y-%m-%dT%H:%M')
            except:
                self.log.error('Error fixing date: %s' % datetext, exc_info=True)
                return None
                

        def calculateNextRun(self, name=None):
            
            # This is all prototype code and should be cleaned up once it is functional
            
            wda=['mon','tue','wed','thu','fri','sat','sun'] # this is stupid but python weekdays start monday
            nextruns={}
            try:
                self.log.info('.. calculating next runs')
                now=datetime.datetime.now()
                for automation in self.automations:
                    if not name or automation==name:
                        old_nextrun=""
                        if 'nextrun' in self.automations[automation]:
                            old_nextrun=self.automations[automation]['nextrun']
                        startdate=None
                        if 'schedules' in self.automations[automation]:
                            for sched in self.automations[automation]['schedules']:
                                try:
                                    startdate = self.fixdate(sched['start'])
        
                                    if sched['type']=='days':
                                        while now>startdate or wda[startdate.weekday()] not in sched['days']:
                                            startdate=startdate+datetime.timedelta(days=1)
        
                                
                                    elif sched['type']=='interval':
                                        if sched['unit']=='days':
                                            idelta=datetime.timedelta(days=int(sched['interval']))
                                        elif sched['unit']=='hours':
                                            idelta=datetime.timedelta(hours=int(sched['interval']))
                                        elif sched['unit']=='min':
                                            idelta=datetime.timedelta(minutes=int(sched['interval']))
                                        elif sched['unit']=='sec':
                                            idelta=datetime.timedelta(seconds=int(sched['interval']))
        
                                        while now>startdate:
                                            startdate=startdate+idelta
                                    
                                    else:
                                        self.log.warn('!. Unsupported type for %s: %s' % (automation, sched['type']))
        
                                    if automation in nextruns:
                                        if startdate < nextruns[automation]:
                                            nextruns[automation]=startdate
                                    else:
                                        if startdate:
                                            nextruns[automation]=startdate
                                    
                                except:
                                    self.log.error('!! Error computing next start for %s' % automation, exc_info=True)
    
                        if startdate:
                            #self.log.info('** %s next start %s %s %s' % (automation, wda[startdate.weekday()], startdate, sched))
                            if old_nextrun!=nextruns[automation].isoformat()+"Z":
                                self.log.info('** changed %s next start to %s %s %s' % (automation, wda[startdate.weekday()], startdate, sched))
                            self.automations[automation]['nextrun']=nextruns[automation].isoformat()+"Z"
                        else:
                            self.automations[automation]['nextrun']=''

                return nextruns
                
            except:
                self.log.error('Error with next run calc', exc_info=True)


        async def buildLogicCommand(self):
            try:
                logicCommand={"logic": {"command": {"Delay": 0, "Alert":0, "Capture":"", "Reset":"", "Wait":0}}}
                await self.dataset.ingest(logicCommand)
            except:
                self.log.error('Error adding logic commands', exc_info=True)


        async def fixAutomationTypes(self):
            
            try:
                for auto in self.automations:
                    changes=False
                    for trigger in self.automations[auto]['triggers']:
                        if "type" not in trigger:
                            trigger['type']="property"
                            changes=True
                    for cond in self.automations[auto]['conditions']:
                        if "type" not in cond:
                            cond['type']="property"
                            changes=True
                    for action in self.automations[auto]['actions']:
                        if "type" not in action:
                            action['type']="command"
                            changes=True
                        elif action['type']=='property':
                            action['type']='command'
                            changes=True
                    
                    if changes:
                        await self.saveAutomation(auto, json.dumps(self.automations[auto]))
            except:
                self.log.error('Error fixing automations', exc_info=True)
         
        async def fixScenes(self, scenes):
            
            try:
                newscenes={}
                for scene in scenes:
                    newscene={}
                    newscene['endpointId']='logic:scene:%s' % scene
                    newscene['friendlyName']=scene
                    newscene['children']={}
                    for child in scenes[scene]:
                        dev=self.dataset.getDeviceByfriendlyName(child)
                        newscene['children'][dev['endpointId']]=scenes[scene][child]
                        
                    newscenes[scene]=newscene
                    changes=False
                
                self.saveJSON('newscenes',newscenes)
                return newscenes
            except:
                self.log.error('Error fixing scenes', exc_info=True)
                
        async def get_since(self, endpointId, data):

            try:
                #adapterport=8094
                #adapterhost='home.dayton.home'
                #url="http://%s:%s/list/last/%s/%s/%s" % (adapterhost, adapterport, endpointId, data['prop'], data['value'])
                path="list/influx/last/%s/%s/%s" % (endpointId, data['prop'], data['value'])
                result=await self.dataset.restGet(path=path)
                if 'time' in result:
                    return result['time']
             
                #async with aiohttp.ClientSession() as client:
                #    async with client.get(url) as response:
                #        result=await response.read()
                #        timedata=json.loads(result.decode())
                #        if 'time' in timedata:
                #            return timedata['time']
                #        else:
                #            return 'unknown'
            except:
                self.log.error('Error getting since time for %s' % endpointId, exc_info=True)
            return "unknown"
                 

        async def get_automations_from_directory(self):
            
            self.log.info('.. Loading automations')
            automations={}
            try:
                automation_files=os.listdir(self.config.automation_directory)
                for filename in automation_files:
                    try:
                        async with aiofiles.open(os.path.join(self.config.automation_directory, filename), mode='r') as automation_file:
                            result = await automation_file.read()
                            automation=json.loads(result)
                            automation['filename']=filename
                            automations[automation['name']]=automation
                    except:
                        self.log.error('.. error getting automation from %s' % filename)
            except:
                self.log.error('An error occurred while getting automations from directory: %s' % self.config.automation_directory, exc_info=True)
                
            return automations


        async def get_data_from_directory(self, data_type):
        
            data_path=""
            data={}
            try:
                data_path=getattr(self.config, '%s_directory' % data_type)
                self.log.info('.. Loading %s from directory %s ' % (data_type, data_path))
                files=os.listdir(data_path)
                for filename in files:
                    try:
                        if os.path.isfile(os.path.join(data_path, filename)):
                            async with aiofiles.open(os.path.join(data_path, filename), mode='r') as data_file:
                                result = await data_file.read()
                                item=json.loads(result)
                                item['filename']=filename
                                data[item['name']]=item
                    except:
                        self.log.error('.. error getting %s from %s' % (data_type, filename))
            except:
                self.log.error('An error occurred while getting %s from directory: %s' % (data_type, data_path), exc_info=True)
            return data

        async def save_data_to_directory(self, data_type, item_name, data):
        
            try:
                data_path=getattr(self.config, '%s_directory' % data_type)
                self.log.info('.. Saving %s %s to directory %s: %s ' % (data_type, item_name, data_path, data))
                jsonfile = open(os.path.join(data_path, '%s.json' % item_name), 'wt')
                json.dump(data, jsonfile, ensure_ascii=False, default=self.jsonDateHandler)
                jsonfile.close()
                #await self.dataset.ingest({ data_type: { item_name : data }}, overwriteLevel='/%s/%s' % (data_type, item_name))
                
            except:
                self.log.error('!! Error saving data to directory: %s %s %s' % (data_type, item_name, data),exc_info=True)

            return data


        async def convert_scene_files(self, scene_directory):
            try:
                for name in self.oldscenes:
                    async with aiofiles.open(os.path.join(scene_directory, name+".json"), 'w') as f:
                        self.log.info('converting %s' % name)
                        self.oldscenes[name]['name']=name
                        self.oldscenes[name]['filename']=name+".json"
                        await f.write(json.dumps(self.oldscenes[name]))
            except:
                self.log.error('!! error converting scenes', exc_info=True)

        async def convert_old_areas(self, data_type="areas"):
            try:
                data_path=getattr(self.config, '%s_directory' % data_type)
                for name in self.areas:
                    async with aiofiles.open(os.path.join(data_path, name+".json"), 'w') as f:
                        self.log.info('fixing %s' % name)
                        working=self.areas[name]
                        if 'newshortcuts' in working:
                            working['shortcuts']=list(working['newshortcuts'])
                            del working['newshortcuts']
                        if 'lights' in working:
                            del working['lights']
                        if 'scenes' in working:
                            if type(working['scenes'])==dict:
                                newscenes=[]
                                for scene in working['scenes']:
                                    newscenes.append(working['scenes'][scene]['endpointId'])
                                working['scenes']=newscenes
                            for scene in working['scenes']:
                                if scene not in working['children']:
                                    working['children'].append(scene)
                            del working['scenes']
                        self.log.info('Saving area: %s %s' % (name, working))
                        await f.write(json.dumps(working))

            except:
                self.log.error('!! error converting scenes', exc_info=True)

        async def pre_activate(self):
            
            self.polltime=1
            self.maintenance_poll_time=120
            self.log.info('.. Logic Manager pre-activation')
            try:
                self.automations={}
                self.mailconfig=self.loadJSON('mail')
                self.mailsender=mailSender(self.log, self.mailconfig)
                self.users=self.loadJSON('users')
                self.modes=self.loadJSON('modes')
                self.areas=await self.get_data_from_directory('area')
                self.scenes=await self.get_data_from_directory('scene')
                self.security=self.loadJSON('security')
                self.automations=await self.get_data_from_directory('automation')
                self.regions=self.loadJSON('regions')
                self.capturedDevices={}
                self.calculateNextRun()
                await self.buildLogicCommand()
                
                for scene in self.scenes:
                    await self.dataset.ingest({"scene": { scene : self.scenes[scene] }})

                for area in self.areas:
                    await self.dataset.ingest({"area": { area : self.areas[area] }})
                    
                if 'All' not in self.areas:
                    await self.dataset.ingest({"area": { "All" : { "scene": "", "children": list(self.areas.keys()), "name": "All" }}})
                    
                for auto in self.automations:
                    await self.dataset.ingest({"activity": { auto : self.automations[auto] }})

                for mode in self.modes:
                    await self.dataset.ingest({"mode": { mode : self.modes[mode] }})
                    
                self.busy=False
                self.maintenance_last=datetime.datetime.now()
                
            except GeneratorExit:
                self.running=False    
            except:
                self.log.error('Error loading cached devices', exc_info=True)

        async def post_activate(self):
            try:
                for item in self.since:
                    self.since[item]['time']=await self.get_since(item,self.since[item])
                self.polling_task = asyncio.create_task(self.pollSchedule())    
                self.log.info('.. tracking time since for items: %s' % self.since)
            except:
                self.log.error('Error during post_activate', exc_info=True)
            

                
        async def start(self):
            self.log.info('.. Starting Logic Manager')
            #try:
            #    self.polling_task = asyncio.create_task(self.pollSchedule())
            #
            #except GeneratorExit:
            #    self.running=False    
            #except:
            #    self.log.error('Error loading cached devices', exc_info=True)
                
        async def pollSchedule(self):
            
            while self.running:
                try:
                    await self.checkScheduledItems()
                    #self.log.info('acp: %s %s' % (self.area_calc_pending, self.busy))
                    if self.area_calc_deferred and self.busy==False:
                        for area in self.area_calc_deferred:
                            bestscene=await self.calculateAreaLevel(area)
                            
                    if ( (datetime.datetime.now()-self.maintenance_last).total_seconds() >self.maintenance_poll_time ):
                        await self.maintenance()
                        self.maintenance_last=datetime.datetime.now()
                    await asyncio.sleep(self.polltime)
                except GeneratorExit:
                    self.running=False
                except:
                    self.log.error('Error polling schedule', exc_info=True)
                    self.running=False


        async def maintenance(self):
            
            maint_items=[]

            for area in self.areas:
                try:
                    if 'children' in self.areas[area]:
                        for dev in self.areas[area]['children']:
                            if dev not in self.dataset.devices:
                                item={"problem": "Missing Device", "source": area, "type": "area", "endpointId": dev }
                                if item and item not in maint_items:
                                    if dev in self.config.fixes:
                                        self.areas[area]['children'].remove(dev)
                                        self.areas[area]['children'].append(self.config.fixes[dev])
                                        item['resolved']=True
                                        await self.save_data_to_directory('area', area, self.areas[area])
                                    maint_items.append(item)
                except:
                    self.log.error('!! Error maintenance checking area %s' % area, exc_info=True)
                    
            for scene in self.scenes:
                try:
                    if 'children' in self.scenes[scene]:
                        for dev in self.scenes[scene]['children']:
                            if dev not in self.dataset.devices:
                                item={"problem": "Missing Device", "source": scene, "type": "scene", "endpointId": dev }
                                if item and item not in maint_items:
                                    if dev in self.config.fixes:
                                        self.scenes[scene]['children'].remove(dev)
                                        self.scenes[scene]['children'].append(self.config.fixes[dev])
                                        item['resolved']=True
                                        await self.save_data_to_directory('scene', scene, self.scenes[scene])
                                    maint_items.append(item)
                except:
                    self.log.error('!! Error maintenance checking scene %s' % scene, exc_info=True)


            for automation in self.automations:
                try:
                    automation_change=False
                    if 'triggers' in self.automations[automation]:
                        for trigger in self.automations[automation]['triggers']:
                            if trigger['endpointId'] not in self.dataset.devices:
                                item={"problem": "Missing Device", "source": automation, "type": "trigger", "endpointId": trigger['endpointId'] }
                                if item and item not in maint_items:
                                    if trigger['endpointId'] in self.config.fixes:
                                        trigger['endpointId']=self.config.fixes[trigger['endpointId']]
                                        item['resolved']=True
                                        automation_change=True
                                    maint_items.append(item)

                                    
                    if 'conditions' in self.automations[automation]:
                        for condition in self.automations[automation]['conditions']:
                            if condition['endpointId'] not in self.dataset.devices:
                                item={"problem": "Missing Device", "source": automation, "type": "condition", "endpointId": condition['endpointId'] }
                                if item and item not in maint_items:
                                    if condition['endpointId'] in self.config.fixes:
                                        condition['endpointId']=self.config.fixes[condition['endpointId']]
                                        item['resolved']=True
                                        automation_change=True
                                    maint_items.append(item)                                    
                    if 'actions' in self.automations[automation]:
                        for action in self.automations[automation]['actions']:
                            if action['endpointId'] not in self.dataset.devices:
                                item={"problem": "Missing Device", "source": automation, "type": "action", "endpointId": action['endpointId'] }
                                if item and item not in maint_items:
                                    if action['endpointId'] in self.config.fixes:
                                        action['endpointId']=self.config.fixes[action['endpointId']]
                                        item['resolved']=True
                                        automation_change=True
                                    maint_items.append(item)
                                    
                    if automation_change:
                        await self.save_data_to_directory('automation', automation, self.automations[automation])       
        
                except:
                    self.log.error('Error checking automation %s' % automation, exc_info=True)
            
            self.dataset.lists['maintenance']=maint_items
                
        async def checkScheduledItems(self):
            try:
                now = datetime.datetime.now()
                for automation in self.automations:
                    try:
                        if 'nextrun' in self.automations[automation]:
                            if self.automations[automation]['nextrun']:
                                #self.log.info('checking %s for %s vs %s (%s)' % (automation, now, self.fixdate(self.automations[automation]['nextrun']), self.automations[automation]['nextrun']))
                                if now>self.fixdate(self.automations[automation]['nextrun']):
                                    self.log.info('Scheduled run is due: %s %s' % (automation,self.automations[automation]['nextrun']))
                                    autodevice=self.dataset.getDeviceByEndpointId('logic:activity:%s' % automation)
                                    await autodevice.SceneController.Activate()
                                    #await self.sendAlexaCommand('Activate', 'SceneController', 'logic:activity:%s' % automation)  
                                    self.automations[automation]['lastrun']=now.isoformat()+"Z"
                                    self.calculateNextRun(automation)
                                    await self.saveAutomation(automation, json.dumps(self.automations[automation]))
                    except:
                        self.log.error('Error checking schedule for %s' % automation, exc_info=True)
            except:
                self.log.error('Error checking scheduled items', exc_info=True)
                    
                
        # Adapter Overlays that will be called from dataset
        async def addSmartDevice(self, path):
            
            try:
                device_id=path.split("/")[2]
                device_type=path.split("/")[1]
                endpointId="%s:%s:%s" % ("logic", device_type, device_id)
                nativeObject=self.dataset.nativeDevices[device_type][device_id]
                if endpointId not in self.dataset.localDevices:
                    if path.split("/")[1]=="activity":
                        return await self.addSimpleActivity(device_id, nativeObject)
                    elif path.split("/")[1]=="mode":
                        return await self.addSimpleMode(device_id, nativeObject)
                    elif path.split("/")[1]=="scene":
                        return await self.addSimpleScene(device_id, nativeObject)
                    elif path.split("/")[1]=="logic":
                        return await self.addLogicCommand(device_id, nativeObject)
                    elif path.split("/")[1]=="area":
                        return await self.addArea(device_id, nativeObject)
                else:
                    self.log.error('Not adding: %s' % path)

            except:
                self.log.error('Error defining smart device', exc_info=True)
            return False
                
        async def addArea(self, device_id, nativeObject):
            device=devices.alexaDevice('logic/area/%s' % device_id, device_id, displayCategories=["AREA"], description="Sofa Logic Command", adapter=self)
            device.AreaController=logic.AreaController(device=device)
            return self.dataset.add_device(device)

        async def addLogicCommand(self, device_id, nativeObject):
        
            device=devices.alexaDevice('logic/logic/command', 'Logic', displayCategories=["LOGIC"], description="Sofa Logic Command", adapter=self)
            device.LogicController=logic.LogicController(device=device)
            return self.dataset.add_device(device)

        async def addSimpleMode(self, device_id, nativeObject):
            device=devices.alexaDevice('logic/mode/%s' % device_id, device_id, displayCategories=['MODE'], description="Sofa Logic Mode", adapter=self)
            device.PowerController=logic.PowerController(device=device)
            device.EndpointHealth=logic.EndpointHealth(device=device)
            return self.dataset.add_device(device)
                
        async def addSimpleActivity(self, device_id, nativeObject):
            device=devices.alexaDevice('logic/activity/%s' % device_id, device_id, displayCategories=["ACTIVITY_TRIGGER"], description="Sofa Logic Activity", adapter=self)
            device.SceneController=logic.SceneController(device=device)
            return self.dataset.add_device(device)

        async def addSimpleScene(self, device_id, nativeObject):
            device=devices.alexaDevice('logic/scene/%s' % device_id, device_id, displayCategories=["SCENE_TRIGGER"], description="Sofa Logic Activity", adapter=self)
            device.SceneController=logic.SceneController(device=device)
            return self.dataset.add_device(device)
            
            
        async def sendAlexaDirective(self, action, trigger={}):
            try:
                payload={}
                url=None
                if 'value' in action:
                    payload=action['value']
                instance=None
                if 'instance' in action:
                    instance=action['instance']
                if 'url' in action:
                    url=action['url']
                return await self.sendAlexaCommand(action['command'], action['controller'], action['endpointId'], payload=payload, instance=instance, trigger=trigger, url=url)
            except:
                self.log.error('Error sending alexa directive: %s' % action, exc_info=True)
                return {}


        async def sendAlexaCommand(self, command, controller, endpointId, payload={}, cookie={}, trigger={}, instance=None, url=None):
            
            try:
                if trigger and command in ['Activate','Deactivate']:
                    cookie["trigger"]=trigger

                header={"name": command, "namespace":"Alexa." + controller, "payloadVersion":"3", "messageId": str(uuid.uuid1()), "correlationToken": str(uuid.uuid1())}
                endpoint={"endpointId": endpointId, "cookie": cookie, "scope":{ "type":"BearerToken", "token":"access-token-from-skill" }}

                if instance!=None:
                    header['instance']=instance

                data={"directive": {"header": header, "endpoint": endpoint, "payload": payload }}
                
                changereport=await self.dataset.sendDirectiveToAdapter(data, url=url)
                return changereport
            except:
                self.log.error('Error executing Alexa Command: %s %s %s %s' % (command, controller, endpointId, payload), exc_info=True)
                return {}

        async def captureSceneFromArea(self, areaid, scenename):
            
            try:
                capdevs={}
                areaprops=await self.dataset.requestReportState(areaid)
                for areaprop in areaprops['context']['properties']:
                    if areaprop['name']=='children':
                        children=areaprop['value']
                        for dev in children:
                            device=self.dataset.getDeviceByEndpointId(dev)
                            if device and 'LIGHT' in device['displayCategories']:
                                cdev={}
                                devprops=await self.dataset.requestReportState(device['endpointId'])
                                for prop in devprops['context']['properties']:
                                    if prop['name']=='powerState':
                                        cdev['powerState']=prop['value']
                                    elif prop['name']=='brightness':
                                        cdev['brightness']=prop['value']
                                    elif prop['name']=='color':
                                        cdev['hue']=prop['value']['hue']
                                        cdev['saturation']=prop['value']['saturation']
                                if cdev:
                                    capdevs[device['endpointId']]=cdev
                if capdevs:
                    self.log.info('Captured: %s' % capdevs)
                    if await self.saveScene(scenename, capdevs):
                        await self.dataset.ingest({"scene": { scenename : self.scenes[scenename] }})
                        areaname=areaid.split(':')[2]
                        self.log.info('Adding scene %s to area %s' % (scenename,areaname))
                        if 'logic:scene:%s' % scenename not in self.areas[areaname]['children']:
                            self.areas[areaname]['children'].append('logic:scene:%s' % scenename)
                            await self.saveArea(areaname, self.areas[areaname])
                            await self.dataset.ingest(self.areas[areaname]['children'], overwriteLevel='/area/%s/children' % areaname)
                                        
            except:
                self.log.error('Error snapshotting device state', exc_info=True)


        async def findStateForCondition(self, controller, propertyName, deviceState):
            
            try:
                for prop in deviceState:
                    if propertyName==prop['name'] and controller==prop['namespace'].split('.')[1]:
                        #self.log.info('Returning prop: %s' % prop)
                        return prop
                
                return False
            except:
                self.log.error('Error finding state for condition: %s %s' % (propertyName, deviceState), exc_info=True)


        def compareCondition(self, conditionValue, operator, propertyValue):
            
            found_val=True
            try:
                for val in conditionValue:
                    if val not in propertyValue:
                        found_val=False
                        break
                    
                    trig_val=conditionValue[val]
                    change_val=propertyValue[val]
                    
                    if type(trig_val)==dict and 'value' in trig_val and 'value' in change_val:
                        trig_val=trig_val['value']
                        change_val=change_val['value']
                        #self.log.info('.. switching to deep values: %s %s %s' % (change_val, operator, trig_val))
                    
                    if operator in ['=', '==']:
                        if trig_val!=change_val:
                            found_val=False
                            break

                    if operator=='!=':
                        if trig_val==change_val:
                            found_val=False
                            break

                    if operator=='>':
                        if trig_val >= change_val:
                            found_val=False
                            break

                    if operator=='>=':
                        if trig_val > change_val:
                            found_val=False
                            break
                        
                    if operator=='<':
                        if trig_val <= change_val:
                            found_val=False
                            break

                    if operator=='<=':
                        if trig_val < change_val:
                            found_val=False
                            break

                    if operator=='contains':
                        if str(change_val) in str(trig_val):
                            return True
            except:
                self.log.error('Error comparing condition: %s %s %s' % (conditionValue, operator, propertyValue), exc_info=True)
                found_val=False 
                        
            return found_val


        async def checkLogicConditions(self, conditions, activityName=""):
            
            try:
                devstateCache={}
                conditionMatch=True
                for condition in conditions:
                    #self.log.info('.. checking condition: %s' % condition)
                    if condition['endpointId'] not in self.dataset.devices and condition['endpointId'] not in self.dataset.localDevices:
                        self.log.info('.. endpoint not in devices: %s %s' % ( condition['endpointId'], self.dataset.devices))
                        conditionMatch=False
                        devstate={}        
                    else:
                        try:
                            if condition['endpointId'] not in devstateCache:
                                devstate=await self.dataset.requestReportState(condition['endpointId'])
                                #self.log.info('devstate for %s: %s' % (condition, devstate))
                                devstateCache[condition['endpointId']]=devstate['context']['properties']
                            else:
                                self.log.info('cached devstate for %s: %s' % ( condition['endpointId'], devstateCache[condition['endpointId']] ))

                            devstate=devstateCache[condition['endpointId']]
                        except KeyError:
                            conditionMatch=False
                            devstate={}
                    prop=await self.findStateForCondition(condition['controller'], condition['propertyName'], devstate)
                    if prop==False or prop==None:
                        self.log.info('!. %s did not find property for condition: %s vs %s' % (activityName, condition, devstate))
                        conditionMatch=False
                        break
                    
                    if 'end' in condition['value']:

                        st=datetime.datetime.strptime(condition['value']['start'],"%H:%M").time()
                        et=datetime.datetime.strptime(condition['value']['end'],"%H:%M").time()
                        if type(prop['value'])==str:
                            ct=datetime.datetime.strptime(prop['value'],"%H:%M:%S.%f").time()
                        else:
                            ct=prop['value']
                        if et<st:
                            self.log.info('End time before start time: %s to %s' % (st,et))
                            if ct>st or ct<et:
                                #self.log.info('Passed alternate time check: %s>%s or %s<%s' % (ct, st, ct,et))
                                pass
                            else:
                                #self.log.info('Failed alternate time check: %s>%s or %s<%s' % (ct, st, ct,et))
                                conditionMatch=False
                                break
                        elif st<ct and et>ct:
                            #self.log.info('Passed time check: %s<%s<%s' % (st, ct, et))
                            pass
                        else:
                            self.log.info('Failed time check: %s<%s<%s' % (st, ct, et))
                            conditionMatch=False
                            break
                        
                    else:
                        condval=condition['value']
                        propval={ prop["name"] : prop["value"] }
                        #if 'value' in condition['value']:
                        #    condval=condition['value']['value']

                        if 'operator' not in condition:
                            self.log.info('No operator in condition %s for %s %s' % (condition, activityName, conditions))
                            condition['operator']='=='
                        if not self.compareCondition(condval, condition['operator'], propval):
                            self.log.info('!. %s did not meet condition: %s %s %s' % (activityName, propval, condition['operator'], condval))
                            conditionMatch=False
                            break

                return conditionMatch

            except:
                self.log.error('Error with chunky Activity', exc_info=True)  
                return False
                
        async def runActivity(self, activityName, trigger={}, triggerEndpointId='', conditions=True, run_id=None):
            
            try:
                if not run_id:
                    run_id="%s-%s" % (activityName, datetime.datetime.now())
                    
                self.running_activities[run_id]={ "name": activityName, "conditions": conditions, "start": datetime.datetime.now(), "state":"start", "active":True }
                if 'conditions' in self.automations[activityName] and conditions==True:
                    if not await self.checkLogicConditions(self.automations[activityName]['conditions'], activityName=activityName):
                        self.running_activities[run_id]["state"]="failed_condition"
                        self.running_activities[run_id]["active"]=False
                        return False

                self.busy=True                
                activity=self.automations[activityName]['actions']
                chunk=[]
                self.running_activities[run_id]["state"]="chunk"
                for action in activity:
                    result=[]
                    if action['command']=="Delay":
                        chunk.append(action)
                        self.running_activities[run_id]["state"]="delay"
                        result=await self.runActivityChunk(chunk)
                        #self.log.info('Result of Pre-Delay chunk: %s' % result)
                        chunk=[]
                    elif action['command']=="Wait":
                        self.running_activities[run_id]["state"]="wait"
                        result=await self.runActivityChunk(chunk)
                        #self.log.info('Result of chunk: %s' % result)
                        chunk=[]
                    elif action['command']=="Alert":
                        self.running_activities[run_id]["state"]="alert"
                        alert=copy.deepcopy(action)
                        if trigger:
                            self.log.info('Trigger: %s' % trigger)
                            if 'endpointId' in trigger:
                                device=self.dataset.getDeviceByEndpointId(trigger['endpointId'])
                                alert['value']['message']['text']=alert['value']['message']['text'].replace('[deviceName]',device['friendlyName'])
                            if 'value' in trigger:
                                avals={'DETECTED':'open', 'NOT_DETECTED':'closed'}
                                alert['value']['message']['text']=alert['value']['message']['text'].replace('[value]',avals[trigger['value']])
                        self.log.info('Result of Alert Macro: %s vs %s / trigger: %s ' % (alert,action, trigger))
                        chunk.append(alert)
                                
                    else:
                        self.running_activities[run_id]["state"]="chunk"
                        chunk.append(action)
                        
                    await self.checkActivityErrors(result)
                    
                if chunk:
                    result=await self.runActivityChunk(chunk)
                    await self.checkActivityErrors(result)
                
                self.running_activities[run_id]["state"]="finish"
                self.automations[activityName]['lastrun']=datetime.datetime.now().isoformat()+"Z"
                await self.saveAutomation(activityName, json.dumps(self.automations[activityName]))
                self.running_activities[run_id]["active"]=False
                self.busy=False   
                return result
                
            except:
                self.log.error('Error with chunky Activity', exc_info=True)

        async def runActivityChunk(self, chunk ):
        
            try:
                allacts = await asyncio.gather(*[self.sendAlexaDirective(action) for action in chunk ])
                return allacts
            except:
                self.log.error('Error executing activity', exc_info=True)


        async def checkActivityErrors(self, results):
            try:
                for result in results:
                    if 'event' not in result:
                        self.log.warning('!. activity item response does not contain an event: %s' % result)
                        
                    elif result['event']['header']['name']=='ErrorResponse':
                        self.log.error('!! Error in activity for %s: %s %s' % (result['event']['endpoint']['endpointId'], result['payload']['type'], result['payload']['message']))
                        if result['payload']['type']=='BRIDGE_UNREACHABLE':
                            pass
                            # TODO/CHEESE 10/7/20 - This should update state cache with unreachable on the endpointhealth if the
                            # error response is not already being processed elsewhere first
                            
                            # chasing adapters by portion of endpoint id should be deprecated overall and unnecessary for most 
                            # non-hub adapters, but logic may be special due to activity/scene processing
                            
                            #if act['event']['endpoint']['endpointId'].split(':')[0] not in self.adapters_not_responding:
                            #    self.adapters_not_responding.append(act['event']['endpoint']['endpointId'].split(':')[0])
            except:
                self.log.error('Error handling activity errors', exc_info=True)                            


        async def analyze_scene_actions(self, actions):
            
            try:
                final_actions=list(actions)
                sc={}   
                for action in actions:
                    self.log.debug('.. analyzing scene actions: %s' % action)
                    newact=dict(action)
                    light_adapter_url=await self.dataset.get_url_for_device(action['endpointId'])
                    light_adapter=action['endpointId'].split(':')[0]
                    newact=dict(action)
                    del newact['endpointId']
                    
                    if light_adapter not in sc:
                        #self.log.info('Adding first adapter %s entry %s' % (light_adapter,{ "actions": newact, "endpoints": [action['endpointId']] } ))
                        sc[light_adapter]=[{ "url":light_adapter_url, "actions": newact, "endpoints": [action['endpointId']] }]
                        continue
                    
                    found=False
                    for scene in sc[light_adapter]:
                        if scene['actions']==newact:
                            if action['endpointId'] not in scene['endpoints']:
                                #self.log.info('Adding adapter %s %s entry %s' % (light_adapter, action['endpointId'], scene ))
                                scene['endpoints'].append(action['endpointId'])
                                found=True
                                break
                    if not found:
                        #self.log.info('Adding adapter %s %s entry %s' % (light_adapter, action['endpointId'], { "actions": newact, "endpoints": [action['endpointId']] } ))
                        sc[light_adapter].append({ "url":light_adapter_url, "actions": newact, "endpoints": [action['endpointId']] } )
                
                for la in sc:
                    for scene in sc[la]:
                        if len(scene['endpoints'])<2:
                            #self.log.info('.. dropping scene with 1 item: %s' % scene)
                            sc[la].remove(scene)
                        else:
                            actcont=[]
                            for ac in actions:
                                if ac['controller'] not in actcont:
                                    actcont.append(ac['controller'])
                            groupres=await self.dataset.checkNativeGroup(la, actcont, scene['endpoints'], scene['url'])
                            if 'id' in groupres:
                                self.log.debug('.. groupres: %s' % groupres)
                                self.native_group_cache[groupres['id']]=groupres['members']
                                self.log.debug('.. cached: %s / %s' % (groupres['id'], groupres['members']))
                                #self.log.info('.. preparing to remove grouped endpoints: %s / %s' % (scene['endpoints'], groupres))
                                for action in actions:
                                    if action['endpointId'] in scene['endpoints'] and action['command']==scene['actions']['command']:
                                        #self.log.info('removing solo action: %s' % action)
                                        try:
                                            final_actions.remove(action)
                                        except:
                                            self.log.error('!! error removing %s from %s' % (action, final_actions))
                                group_action=scene['actions']
                                group_action['endpointId']=groupres['id']
                                group_action['url']=scene['url']
                                final_actions.append(group_action)
                            else:
                                self.log.info('.. no matching group from adapter: %s' % groupres)
                                
                
                self.log.debug('.. result of scene analysis: %s' % final_actions)
                return final_actions
            
            except AttributeError:
                self.log.warning('!! native group calculations are disabled (no adapter map')
            
            except:
                self.log.error('!! error during scene analysis', exc_info=True)
            return actions
                    

        async def runScene(self, sceneName):
        
            try:
                self.busy=True
                scene=self.scenes[sceneName]
                acts=[]

                for light in scene['children']:
                        
                    if 'powerState' in scene['children'][light] and scene['children'][light]['powerState']=='OFF':
                        acts.append({'command':'TurnOff', 'controller':'PowerController', 'endpointId':light, 'value': None})
                            
                    elif int(scene['children'][light]['brightness'])==0 and 'powerState' not in scene['children'][light]:
                        acts.append({'command':'TurnOff', 'controller':'PowerController', 'endpointId':light, 'value': None})

                    else:
                        if 'hue' in scene['children'][light]:
                            acts.append({'command':'SetColor', 'controller':'ColorController', 'endpointId':light, "value": { 'color': { 
                                "brightness": scene['children'][light]['brightness']/100,
                                "saturation": scene['children'][light]['saturation'],
                                "hue": scene['children'][light]['hue'] }}})
                        else:
                            acts.append({'command':'TurnOn', 'controller':'PowerController', 'endpointId':light, 'value': None})
                            acts.append({'command':'SetBrightness', 'controller':'BrightnessController', 'endpointId':light, 'value': { "brightness": int(scene['children'][light]['brightness']) }} )


                acts=await self.analyze_scene_actions(acts)
                allacts = await asyncio.gather(*[self.sendAlexaDirective(action) for action in acts ])
                self.log.debug('.. scene %s result: %s' % (sceneName, allacts))    
                self.busy=False
            except:
                self.log.error('Error executing Scene', exc_info=True)


        async def imageGetter(self, item, thumbnail=False):

            try:
                source=item.split('/',1)[0] 
                if source in self.dataset.adapters:
                    result='#'
                    if thumbnail:
                        url = 'http://%s:%s/thumbnail/%s' % (self.dataset.adapters[source]['address'], self.dataset.adapters[source]['port'], item.split('/',1)[1] )
                    else:
                        url = 'http://%s:%s/image/%s' % (self.dataset.adapters[source]['address'], self.dataset.adapters[source]['port'], item.split('/',1)[1] )
                    async with aiohttp.ClientSession() as client:
                        async with client.get(url) as response:
                            result=await response.read()
                            return result
                            #result=result.decode()
                            if str(result)[:10]=="data:image":
                                #result=base64.b64decode(result[23:])
                                self.imageCache[item]=str(result)
                                return result
    
            except concurrent.futures._base.CancelledError:
                self.log.error('Error getting image %s (cancelled)' % item)
            except:
                self.log.error('Error getting image %s' % item, exc_info=True)
            
            return None
                
        async def runAlert(self, message, image=None):
            
            try:
                for user in self.users:
                    if self.users[user]['alerts']:
                        self.mailsender.sendmail(self.users[user]['email'], '', message+' @'+datetime.datetime.now().strftime("%l:%M.%S%P")[:-1], image)
            except:
                self.log.error('Error sending alert', exc_info=True)
                

        def buildTriggerList(self):
            
            triggerlist=[]
            try:
                for automation in self.automations:
                    if 'triggers' in self.automations[automation]:
                        for trigger in self.automations[automation]['triggers']:
                            try:
                                if trigger['type']=='property':
                                    if 'value' in trigger:
                                        trigname="%s.%s.%s=%s" % (trigger['endpointId'], trigger['controller'], trigger['propertyName'], trigger['value'])
                                    else:
                                        trigname="%s.%s.%s" % (trigger['endpointId'], trigger['controller'], trigger['propertyName'])
                                        
                                elif trigger['type']=='event':
                                    trigname="event=%s.%s.%s" % (trigger['endpointId'], trigger['controller'], trigger['propertyName'])
                                    self.log.debug('Event trigger: %s' % trigname)

                                else:
                                    self.log.info('Skipping unknown trigger type: %s' % trigger)
                                    continue
                                if trigname not in triggerlist:
                                    triggerlist[trigname]=[]
                                triggerlist[trigname].append({ 'name':automation, 'type':'automation' })
                            except:
                                self.log.error('Error computing trigger shorthand for %s %s' % (automation,trigger), exc_info=True)
                            
                self.log.debug('Triggers: %s' % (len(triggerlist)))
            except:
                self.log.error('Error calculating trigger shorthand:', exc_info=True)
            
            return triggerlist


        async def runEvents(self, events, change, trigger=''):
        
            try:
                self.busy=True
                actions=[]
                for event in events:
                    self.log.info('.. Triggered Event: %s' % event)
                    if event['type']=='event':
                        action=self.events[event['name']]['action']
                    elif event['type']=='automation':
                        action={"controller": "SceneController", "command":"Activate", "endpointId":"logic:activity:"+event['name']}
                    
                    if "value" in action:
                        aval=action['value']
                    else:
                        action['value']=''
                        
                    actions.append(action)
                    
                allacts = await asyncio.gather(*[self.sendAlexaDirective(action, trigger=change) for action in actions ])
                self.log.info('.. Trigger %s result: %s' % (change, allacts))
                self.busy=False
                return allacts
            except:
                self.log.error('Error executing event reactions', exc_info=True)

        def runEventsThread(self, events, change, trigger='', loop=None):
        
            try:
                actions=[]
                for event in events:
                    action={"controller": "SceneController", "command":"Activate", "endpointId":"logic:activity:"+event, "value":""}
                    actions.append(action)
                
                allacts = asyncio.ensure_future(asyncio.gather(*[self.sendAlexaDirective(action, trigger=change) for action in actions ], loop=loop), loop=loop)
                #self.log.info('.. Trigger %s result: %s' % (change, allacts))
                return allacts
            except:
                self.log.error('Error executing threaded event reactions', exc_info=True)

        async def calculateAreaLevel(self, area):
            
            try:
                if area in self.area_calc_pending:
                    return ''
                    
                if self.busy:
                    if area not in self.area_calc_deferred:
                        self.area_calc_deferred.append(area)
                    return ''  
                    
                area_scenes=[]
                for item in self.areas[area]['children']:
                    if item in self.dataset.localDevices and 'SCENE_TRIGGER' in self.dataset.localDevices[item].displayCategories:
                        if item not in area_scenes:
                            area_scenes.append(item)
                
                if len(area_scenes)<1: # no local scenes in area
                    return ""
              
                #self.log.info('pending: %s' % self.area_calc_pending)
                self.area_calc_pending.append(area)
                
                devstate_cache={}
                highscore=0
                bestscene=""
                #self.log.info('.. calculating area level: %s %s' % (area,self.areas[area]['children']))
                state_reports=await self.request_state_reports(self.areas[area]['children'])
                for child in area_scenes:
                    scenescore=0
                    scene=child.split(':')[2]
                    if scene not in self.scenes:
                        self.log.warning('!! scene %s (in %s) does not exist in scene definition file. This data error should be fixed manually.' % (child,area))
                        continue
                    
                    try:
                        light_reports=await self.request_state_reports(self.scenes[scene]['children'])
                        for light in self.scenes[scene]['children']:
                            devbri=0
                            if 'powerState' in self.scenes[scene]['children'][light] and self.scenes[scene]['children'][light]['powerState']=='OFF':
                                scenebri=0
                            else:
                                scenebri=self.scenes[scene]['children'][light]['brightness']

                            try:
                                if light in self.state_cache:
                                    for prop in self.state_cache[light]:
                                        if prop['name']=="powerState":
                                            if prop['value']=='OFF':
                                                devbri=0
                                                break
                                        elif prop['name']=="brightness":
                                            devbri=prop['value']
                                        elif prop['name']=='connectivity':
                                            if prop['value']['value']=='UNREACHABLE':
                                                devbri=0
                                                break
                                else:
                                    devbri=0
                            except KeyError:
                                self.log.error('!! device not in state cache: %s' % light, exc_info=True)
                                devbri=0
                            
                            scenescore+=(50-abs(devbri-scenebri))

                        # scenes with larger numbers of lights will have higher scores unless its divided by the number of lights
                        scenescore=scenescore / len(self.scenes[scene]['children'])
                        #self.log.info('---- Scene score %s = %s' % (child, scenescore))
                            
                    except:
                        self.log.error('!! error getting light level for area scene compute: %s %s' % (area, child), exc_info=True)
                        scenescore=0
                        break
                        
                    if scenescore>highscore:
                        highscore=scenescore
                        bestscene=child
                
                if area in self.area_calc_pending:
                    self.area_calc_pending.remove(area)
                if area in self.area_calc_deferred:
                    self.area_calc_deferred.remove(area)        
                if bestscene:
                    if bestscene!=self.dataset.nativeDevices['area'][area]['scene']:
                        self.log.info('.. %s scene change %s to %s' % (area, self.dataset.nativeDevices['area'][area]['scene'].split(':')[2], bestscene.split(':')[2]))
                    changes=await self.dataset.ingest({ "area" : { area: {'scene': bestscene }}})
                #self.log.debug('.. best scene: %s' % bestscene)
                return bestscene

            except:
                self.log.error('Error in area scene calc: %s' % (area), exc_info=True)
                return ""
                

        async def virtualAddDevice(self, deviceId, change):
            
            try:
                for area in self.areas:
                    if deviceId in self.areas[area]['children']:
                        #self.log.info('Area %s' % (self.dataset.nativeDevices['area'][area]))
                        bestscene=await self.calculateAreaLevel(area)
            except AttributeError:
                # areas not ready yet
                pass
                #self.log.error('Error in virtual add handler: %s %s' % (deviceId, change), exc_info=True)

            except:
                self.log.error('Error in virtual add handler: %s %s' % (deviceId, change), exc_info=True)
                

        async def trigger_check(self, deviceId, change):
            
            try:
                hits=[]
                for automation in self.automations:
                    if 'triggers' in self.automations[automation]:
                        for trigger in self.automations[automation]['triggers']:
                            if deviceId!=trigger['endpointId']:
                                continue
                            if change['namespace'].split('.')[1]!=trigger['controller']:
                                continue
                            if change['name']!=trigger['propertyName']:
                                continue
                            #self.log.info('So far so good: %s %s' %  (deviceId, change))
                            if change['value']:
                                changevalue={ change['name'] : change['value'] }
                                #self.log.info('values: %s vs %s' % (changevalue, trigger['value']))
                                if 'operator' not in trigger:
                                    trigger['operator']='='
                                if self.compareCondition(trigger['value'], trigger['operator'], changevalue):
                                    self.log.info('!!!!!! Active trigger: %s (%s vs %s)' % (automation, trigger, change))
                                    hits.append(automation) 
                if hits:
                    self.loop.run_in_executor(self.logicpool, self.runEventsThread, hits, change, "", self.loop)
            except:
                self.log.error('Error during new trigger check', exc_info=True)
                                    
                                    
       
        async def virtualChangeHandler(self, deviceId, change):
            
            try:
                # self.log.info('<< vch %s %s' % (deviceId, change))
                now=datetime.datetime.now()
                await self.trigger_check(deviceId, change)
                
                for area in self.areas:
                    if deviceId in self.areas[area]['children']:
                        bestscene=await self.calculateAreaLevel(area)
                    if deviceId in self.native_group_cache:
                        for groupDeviceId in self.native_group_cache[deviceId]:
                            if groupDeviceId in self.areas[area]['children']:
                                bestscene=await self.calculateAreaLevel(area)                            
                        
                if deviceId in self.since:
                    if change['value']==self.since[deviceId]['value'] and change['name']==self.since[deviceId]['prop']:
                        self.since[deviceId]['time']=now.isoformat()+"Z"
                        self.log.info('.. since time update: %s %s' % (deviceId,self.since[deviceId]))

            except:
                self.log.error('Error in virtual change handler: %s %s' % (deviceId, change), exc_info=True)

        #async def virtualEventHandler(self, event, source, deviceId, message):
            
        #    try:
        #        trigname="event=%s.%s.%s" % (deviceId, source, event)
        #        self.log.info('Event trigger: %s' % trigname)
        #        if trigname in self.eventTriggers:
        #            self.log.info('!+ This is an event trigger we are watching for: %s %s' % (trigname, message))
        #            #await self.runEvents(self.eventTriggers[trigname], message, trigname)
        #            self.loop.run_in_executor(self.logicpool, self.runEventsThread, self.eventTriggers[trigname], message, trigname, self.loop)

        #    except:
        #        self.log.error('Error in virtual event handler: %s %s %s' % (event, deviceId, message), exc_info=True)


        async def buildRegion(self, thisRegion=None):
            
            try:
                reg={}
                for region in self.regions:
                    reg[region]={ 'scenes': {}, 'areas':{} }
                    for area in self.regions[region]['areas']:
                        reg[region]['areas'][area]=self.areas[area]
                        for scene in self.areas[area]['scenes']:
                            reg[region]['scenes'][scene]=self.scenes[scene]
                            
                if thisRegion:
                    return reg[thisRegion]
                    
                return reg
            except:
                self.log.info('Error building region data: %s' % region, exc_info=True)
            


        async def virtualList(self, itempath, query={}):

            try:
                self.log.info('<< list request: %s %s' % (itempath, query))


                if itempath in self.dataset.lists:
                    return self.dataset.lists[itempath]

                if itempath=="activity":
                    return self.running_activities
                
                if itempath=="automations":
                    return self.automations

                if itempath=="since":
                    return self.since

                if itempath=='schedule':
                    scheduled=[]
                    for automation in self.automations:
                        try:
                            if self.automations[automation]['nextrun']:
                                scheduled.append({"name":automation, "nextrun":self.automations[automation]['nextrun'], "lastrun": self.automations[automation]['lastrun'], "schedule":self.automations[automation]['schedules']})
                        except:
                            self.log.info('Not including %s' % automation,exc_info=True)
                    ss = sorted(scheduled, key=itemgetter('nextrun'))
                    #ss.reverse()
                    self.log.info('Sched: %s' % ss)
                    return ss                
                if itempath=="scenes":
                    return self.scenes

                if itempath=="security":
                    return self.security

                if itempath=="areas":
                    return self.areas

                #if itempath=="events":
                #    self.loadEvents()
                #    return {"events": self.events, "triggers": self.eventTriggers}
                    
                if itempath=="regions":
                    return await self.buildRegion()
                    
                if itempath=="automationlist":
                    al={}
                    for auto in self.automations:
                        if 'conditions' not in self.automations[auto]:
                            self.automations[auto]['conditions']=[]
                        if 'triggers' not in self.automations[auto]:
                            self.automations[auto]['triggers']=[]
                        if 'favorite' not in self.automations[auto]:
                            self.automations[auto]['favorite']=False

                        al[auto]={ 'favorite':self.automations[auto]['favorite'], 'lastrun': self.automations[auto]['lastrun'], 'triggerCount': len(self.automations[auto]['triggers']), 'actionCount': len(self.automations[auto]['actions']), 'conditionCount': len(self.automations[auto]['conditions']), 'endpointId':'logic:activty:%s' % auto }
                    return al

                if itempath=="arealist":
                    al={}
                    for area in self.areas:
                        al[area]={ 'lights': self.areas[area]['lights'], 'scenes': self.areas[area]['lights'] }
                    return al

                if itempath=="regionlist":
                    al={}
                    for region in self.regions:
                        al[region]={ 'count': len(self.regions[region]['rooms']) }
                    return al
                    
                if '/' in itempath:
                    ip=itempath.split('/')
                    if ip[0]=='automation':
                        if ip[1] in self.automations:
                            return self.automations[ip[1]]
                    if ip[0]=='region':
                        if ip[1] in self.regions:
                            return await self.buildRegion(ip[1])
                            #return self.regions[ip[1]]['rooms']
                    if ip[0]=='area':
                        if ip[1] in self.areas:
                            return self.areas[ip[1]]
                    if ip[0]=='arealights':
                        if ip[1] in self.areas:
                            return self.areas[ip[1]]['lights']
                    if ip[0]=='areascenes':
                        if ip[1] in self.areas:
                            return self.areas[ip[1]]['scenes']
                        else:
                            result={}

                        if ip[1] in self.areas:
                            result['lights']=self.areas[ip[1]]['lights']
                        return result
                    if ip[0]=='scene':
                        if ip[1] in self.scenes:
                            
                            return self.scenes[ip[1]]
                        else:
                            result={}
                    
                return {}

            except:
                self.log.error('Error getting virtual list for %s' % itempath, exc_info=True)
                

if __name__ == '__main__':
    adapter=logic(name='logic')
    adapter.start()