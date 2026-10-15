"""Employee Attendance & Analytics API. Run: uvicorn app.main:app --port 8000"""
import os, re, calendar
from datetime import datetime, date, time, timedelta, timezone
from decimal import Decimal, ROUND_HALF_UP
from typing import Optional, Literal
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Query, Path
from pydantic import BaseModel, Field, ConfigDict, field_validator, model_validator
from pymongo import MongoClient, ASCENDING, DESCENDING, ReturnDocument
from pymongo.errors import DuplicateKeyError, PyMongoError
from bson import ObjectId

load_dotenv()
client = MongoClient(os.getenv("MONGO_URI", "mongodb://localhost:27017"), serverSelectionTimeoutMS=2500)
db = client[os.getenv("MONGO_DB", "attendance_db")]
IST = timezone(timedelta(hours=5, minutes=30)); UTC = timezone.utc
PRESENCE = {"PRESENT", "WFH", "ON_DUTY"}; STATUSES = PRESENCE | {"ABSENT", "LEAVE"}
app = FastAPI(title="Employee Attendance & Analytics API", version="2.0.0")

@app.on_event("startup")
def create_indexes():
    # Unique constraints make check-then-write races safe.
    db.employees.create_index([("emp_code", ASCENDING)], unique=True, name="uq_employee_code")
    db.employees.create_index([("department", ASCENDING), ("joined_on", ASCENDING)], name="department_joined")
    db.employees.create_index([("joined_on", ASCENDING), ("department", ASCENDING)], name="joined_department")
    db.attendance_logs.create_index([("emp_code", ASCENDING), ("date", ASCENDING)], unique=True, name="uq_attendance_employee_date")
    db.attendance_logs.create_index([("date", DESCENDING), ("emp_code", ASCENDING)], name="attendance_date_emp")
    db.attendance_logs.create_index([("emp_code", ASCENDING), ("date", ASCENDING), ("status", ASCENDING)], name="attendance_emp_date_status")
    db.attendance_logs.create_index([("date", ASCENDING), ("status", ASCENDING), ("late_minutes", DESCENDING)], name="attendance_analytics_date")

def now_utc(): return datetime.now(UTC).replace(microsecond=0)
def from_ms(v):
    if v is None: return None
    if isinstance(v, bool) or not isinstance(v, int) or v < 100000000000 or v > 4102444800000: raise HTTPException(422, "timestamp must be epoch milliseconds")
    try: return datetime.fromtimestamp(v/1000, UTC).replace(microsecond=0)
    except (ValueError, OSError, OverflowError): raise HTTPException(422, "invalid timestamp")
def to_ms(v):
    if v is None: return None
    if isinstance(v, (int, float)) and not isinstance(v, bool): return int(v)
    if v.tzinfo is None: v=v.replace(tzinfo=UTC)
    return int(v.timestamp()*1000)
def as_utc(v): return v.replace(tzinfo=UTC) if v and v.tzinfo is None else v
def ist(v): return as_utc(v).astimezone(IST)
def valid_date(s):
    try:
        if not isinstance(s, str) or not re.fullmatch(r'\d{4}-\d{2}-\d{2}', s): raise ValueError()
        return date.fromisoformat(s)
    except Exception: raise HTTPException(422, "date must be YYYY-MM-DD")
def valid_month(s):
    if not re.fullmatch(r"\d{4}-(0[1-9]|1[0-2])", s or ""): raise HTTPException(422, "month must be YYYY-MM")
    y,m=map(int,s.split('-')); return date(y,m,1), date(y,m,calendar.monthrange(y,m)[1])
def round_half(v, places=2):
    if v is None: return None
    return float(Decimal(str(v)).quantize(Decimal('1.'+'0'*places), rounding=ROUND_HALF_UP))
def compute_late_minutes(punch_in, shift_start):
    local=ist(punch_in); h,m=map(int,shift_start.split(':')); start=local.replace(hour=h,minute=m,second=0,microsecond=0)
    if h*60+m <= int(shift_start.split(':')[0])*60+int(shift_start.split(':')[1]): pass
    mins=int((local-start).total_seconds()//60)
    return mins if (local-start).total_seconds()>600 else 0
def shift_bounds(punch_in, emp):
    local=ist(punch_in); sh,sm=map(int,emp['shift_start'].split(':')); eh,em=map(int,emp['shift_end'].split(':'))
    start=local.replace(hour=sh,minute=sm,second=0,microsecond=0)
    if (eh*60+em) <= (sh*60+sm):
        # Early-morning punch belongs to previous day's overnight shift.
        if local.hour*60+local.minute < eh*60+em: start-=timedelta(days=1)
        end=start.replace(hour=eh,minute=em)+timedelta(days=1)
    else: end=start.replace(hour=eh,minute=em)
    return start,end
def late_for(punch, emp):
    local=ist(punch); start,_=shift_bounds(punch,emp); delta=(local-start).total_seconds()
    return int(delta//60) if delta>600 else 0
def overtime_for(punch_out, punch_in, emp):
    _,end=shift_bounds(punch_in,emp); delta=(ist(punch_out)-end).total_seconds()
    mins=int(delta//60)
    return mins if mins>=30 else 0
def work_hours(pin,pout): return round_half((pout-pin).total_seconds()/3600,2)
def enrich(doc):
    if not doc:return doc
    d=dict(doc); d.pop('_id',None)
    for k in ('punch_in','punch_out'): d[k]=to_ms(d.get(k))
    d['history']=[{**h,'at':to_ms(h.get('at')),'changes':{k:{**v, **({'from':to_ms(v.get('from')),'to':to_ms(v.get('to'))} if k in ('punch_in','punch_out') else {})} for k,v in h.get('changes',{}).items()}} for h in d.get('history',[])]
    return d
def page_args(page, page_size):
    if page<1 or page_size<1 or page_size>100: raise HTTPException(422,"page must be >=1 and page_size must be 1..100")
def date_filter(date_from,date_to):
    q={}
    if date_from or date_to:
        q['date']={}
        if date_from: valid_date(date_from); q['date']['$gte']=date_from
        if date_to: valid_date(date_to); q['date']['$lte']=date_to
        if date_from and date_to and date_from>date_to: raise HTTPException(422,"date_from must not exceed date_to")
    return q

def aggregation_month_match(month):
    start,end=valid_month(month); return {'date':{'$gte':start.isoformat(),'$lte':end.isoformat()}},start,end
def is_weekday(d): return d.weekday()<5
def pipeline_employee_monthly(emp_code, month):
    match,start,end=aggregation_month_match(month)
    # Workday count is computed inside MongoDB, not by iterating in the API.
    return [
      {'$match':{'emp_code':emp_code}},
      {'$lookup':{'from':'attendance_logs','let':{'code':'$emp_code'},'pipeline':[{'$match':{'$expr':{'$and':[{'$eq':['$emp_code','$$code']},{'$gte':['$date',start.isoformat()]},{'$lte':['$date',end.isoformat()]}]}}}], 'as':'logs'}},
      {'$unwind':{'path':'$logs','preserveNullAndEmptyArrays':True}},
      {'$addFields':{'_d':{'$cond':[{'$ne':[{'$ifNull':['$logs.date',None]},None]},{'$dateFromString':{'dateString':'$logs.date'}},None]},'_join':{'$dateFromString':{'dateString':'$joined_on'}}}},
      {'$group':{'_id':'$emp_code','joined_on':{'$first':'$_join'},'days':{'$addToSet':{'$cond':[{'$and':[{'$ne':['$_d',None]},{'$gte':['$_d',{'$dateFromString':{'dateString':start.isoformat()}}]},{'$lte':['$_d',{'$dateFromString':{'dateString':end.isoformat()}}]}]},'$logs',None]}},'records':{'$push':'$logs'}}},
      {'$project':{'_id':0,'emp_code':'$_id','joined_on':1,'days':{'$filter':{'input':'$days','as':'x','cond':{'$ne':['$$x',None]}}},'records':1,
       'working_days':{'$size':{'$filter':{'input':{'$map':{'input':{'$range':[0,{'$max':[0,{'$dateDiff':{'startDate':{'$max':['$joined_on',{'$dateFromString':{'dateString':start.isoformat()}}]},'endDate':{'$dateAdd':{'startDate':{'$dateFromString':{'dateString':end.isoformat()}},'unit':'day','amount':1}},'unit':'day'}}]}]},'as':'i','in':{'$dateAdd':{'startDate':{'$max':['$joined_on',{'$dateFromString':{'dateString':start.isoformat()}}]},'unit':'day','amount':'$$i'}}}},'as':'x','cond':{'$and':[{'$gte':[{'$dayOfWeek':'$$x'},2]},{'$lte':[{'$dayOfWeek':'$$x'},6]}]}}}},
       'present_days':{'$sum':{'$map':{'input':{'$filter':{'input':'$records','as':'r','cond':{'$in':['$$r.status',list(PRESENCE)]}}},'as':'r','in':{'$cond':[{'$and':[{'$gte':[{'$dayOfWeek':{'$dateFromString':{'dateString':'$$r.date'}}},2]},{'$lte':[{'$dayOfWeek':{'$dateFromString':{'dateString':'$$r.date'}}},6]}]},{'$cond':[{'$eq':[{'$ifNull':['$$r.half_day',False]},True]},0.5,1]},0]}}}},
       'leave_days':{'$size':{'$filter':{'input':'$records','as':'r','cond':{'$eq':['$$r.status','LEAVE']}}}},
       'late_count':{'$size':{'$filter':{'input':'$records','as':'r','cond':{'$gt':[{'$ifNull':['$$r.late_minutes',0]},0]}}}},
       'total_late_minutes':{'$sum':{'$map':{'input':'$records','as':'r','in':{'$ifNull':['$$r.late_minutes',0]}}}},'total_overtime_minutes':{'$sum':{'$map':{'input':'$records','as':'r','in':{'$ifNull':['$$r.overtime_minutes',0]}}}}}},
      {'$addFields':{'attendance_pct':{'$cond':[{'$gt':['$working_days',0]},{'$round':[{'$multiply':[{'$divide':['$present_days','$working_days']},100]},2]},None]}}}
    ]

def pipeline_department_summary(month, department=None):
    start,end=valid_month(month); match={'joined_on':{'$lte':end.isoformat()}}
    if department: match['department']=department
    return [ {'$match':match},
      {'$lookup':{'from':'attendance_logs','let':{'code':'$emp_code'},'pipeline':[{'$match':{'$expr':{'$and':[{'$eq':['$emp_code','$$code']},{'$gte':['$date',start.isoformat()]},{'$lte':['$date',end.isoformat()]}]}}}], 'as':'logs'}},
      {'$unwind':{'path':'$logs','preserveNullAndEmptyArrays':True}},
      {'$addFields':{'_weekday':{'$cond':[{'$ne':[{'$ifNull':['$logs.date',None]},None]},{'$and':[{'$gte':[{'$dayOfWeek':{'$dateFromString':{'dateString':'$logs.date'}}},2]},{'$lte':[{'$dayOfWeek':{'$dateFromString':{'dateString':'$logs.date'}}},6]}]},False]}}},
      {'$group':{'_id':'$department','headcodes':{'$addToSet':'$emp_code'},'present_days':{'$sum':{'$cond':[{'$and':[{'$in':[{'$ifNull':['$logs.status','']},list(PRESENCE)]},'$_weekday']},{'$cond':[{'$eq':[{'$ifNull':['$logs.half_day',False]},True]},0.5,1]},0]}},
       'work_hours_values':{'$push':{'$cond':[{'$and':[{'$in':[{'$ifNull':['$logs.status','']},list(PRESENCE)]},{'$ne':[{'$ifNull':['$logs.work_hours',None]},None]}]},'$logs.work_hours',None]}},
       'late_count':{'$sum':{'$cond':[{'$gt':[{'$ifNull':['$logs.late_minutes',0]},0]},1,0]}},'total_late_minutes':{'$sum':{'$ifNull':['$logs.late_minutes',0]}},'leave_count':{'$sum':{'$cond':[{'$eq':['$logs.status','LEAVE']},1,0]}},'on_duty_count':{'$sum':{'$cond':[{'$eq':['$logs.status','ON_DUTY']},1,0]}}}},
      {'$project':{'_id':0,'department':'$_id','headcount':{'$size':'$headcodes'},'present_days':1,'avg_work_hours':{'$let':{'vars':{'vals':{'$filter':{'input':'$work_hours_values','as':'v','cond':{'$ne':['$$v',None]}}}},'in':{'$cond':[{'$gt':[{'$size':'$$vals'},0]},{'$round':[{'$avg':'$$vals'},2]},None]}}},'late_count':1,'total_late_minutes':1,'leave_count':1,'on_duty_count':1}},
      {'$match':{'headcount':{'$gt':0}}},{'$sort':{'department':1}}]

def pipeline_late_leaderboard(month, department=None, limit=10):
    start,end=valid_month(month)
    pipeline=[{'$match':{'date':{'$gte':start.isoformat(),'$lte':end.isoformat()}}},
      {'$group':{'_id':'$emp_code','total_late_minutes':{'$sum':{'$ifNull':['$late_minutes',0]}},'late_count':{'$sum':{'$cond':[{'$gt':[{'$ifNull':['$late_minutes',0]},0]},1,0]}}}},
      {'$match':{'total_late_minutes':{'$gt':0}}},
      {'$lookup':{'from':'employees','localField':'_id','foreignField':'emp_code','as':'emp'}},{'$unwind':'$emp'}]
    if department: pipeline.append({'$match':{'emp.department':department}})
    pipeline += [{'$setWindowFields':{'sortBy':{'total_late_minutes':-1},'output':{'rank':{'$rank':{}}}}},{'$match':{'rank':{'$lte':limit}}},{'$sort':{'total_late_minutes':-1,'_id':1}},{'$project':{'_id':0,'rank':1,'emp_code':'$_id','name':'$emp.name','department':'$emp.department','total_late_minutes':1,'late_count':1}}]
    return pipeline

def pipeline_trend(department, start_s, end_s):
    start=valid_date(start_s); end=valid_date(end_s)
    # Generate calendar rows in MongoDB using $range; then join daily aggregates and apply window function.
    return [ {'$match':{'department':department}},
      {'$group':{'_id':'$department','joined_dates':{'$push':{'$dateFromString':{'dateString':'$joined_on'}}},'codes':{'$push':'$emp_code'}}},
      {'$project':{'_id':0,'department':'$_id','joined_dates':1,'codes':1,'days':{'$map':{'input':{'$range':[0,{'$add':[{'$dateDiff':{'startDate':{'$dateFromString':{'dateString':start_s}},'endDate':{'$dateFromString':{'dateString':end_s}},'unit':'day'}},1]}]},'as':'i','in':{'$dateToString':{'format':'%Y-%m-%d','date':{'$dateAdd':{'startDate':{'$dateFromString':{'dateString':start_s}},'unit':'day','amount':'$$i'}}}}}}}},
      {'$unwind':'$days'}, {'$set':{'_day':{'$dateFromString':{'dateString':'$days'}}}},
      {'$lookup':{'from':'attendance_logs','let':{'d':'$days','codes':'$codes'},'pipeline':[{'$match':{'$expr':{'$and':[{'$eq':['$date','$$d']},{'$in':['$emp_code','$$codes']}]}}}], 'as':'logs'}},
      {'$addFields':{'headcount':{'$size':{'$filter':{'input':'$joined_dates','as':'j','cond':{'$lte':['$$j','$_day']}}}},'present_count':{'$sum':{'$map':{'input':{'$filter':{'input':'$logs','as':'l','cond':{'$in':['$$l.status',list(PRESENCE)]}}},'as':'l','in':{'$cond':[{'$eq':[{'$ifNull':['$$l.half_day',False]},True]},0.5,1]}}}},'late_count':{'$size':{'$filter':{'input':'$logs','as':'l','cond':{'$gt':[{'$ifNull':['$$l.late_minutes',0]},0]}}}},'is_working_day':{'$and':[{'$gte':[{'$dayOfWeek':'$_day'},2]},{'$lte':[{'$dayOfWeek':'$_day'},6]}]}}},
      {'$addFields':{'attendance_rate':{'$cond':[{'$and':['$is_working_day',{'$gt':['$headcount',0]}]},{'$round':[{'$divide':['$present_count','$headcount']},4]},None]}}},
      {'$setWindowFields':{'sortBy':{'_day':1},'output':{'_avg7':{'$avg':'$attendance_rate','window':{'documents':[-6,0]}}}}},
      {'$project':{'_id':0,'date':'$days','is_working_day':1,'headcount':1,'present_count':1,'late_count':1,'attendance_rate':1,'moving_avg_7d':{'$cond':[{'$eq':[{'$type':'$_avg7'},'missing']},None,{'$round':['$_avg7',4]}]}}}, {'$sort':{'date':1}}]

class EmployeeIn(BaseModel):
    model_config=ConfigDict(extra='forbid')
    emp_code:str=Field(pattern=r'^EMP\d{4,6}$'); name:str=Field(min_length=1,max_length=100); email:str=Field(pattern=r'^[^@\s]+@[^@\s]+\.[^@\s]+$',max_length=120); department:str=Field(min_length=1,max_length=50)
    shift_start:str=Field(default='09:30',pattern=r'^([01]\d|2[0-3]):[0-5]\d$'); shift_end:str=Field(default='18:30',pattern=r'^([01]\d|2[0-3]):[0-5]\d$'); joined_on:str
    @field_validator('joined_on')
    @classmethod
    def check_date(cls,v):
        if not re.fullmatch(r'\d{4}-\d{2}-\d{2}',v): raise ValueError('joined_on must be YYYY-MM-DD')
        date.fromisoformat(v); return v
    @model_validator(mode='after')
    def shift_diff(self):
        if self.shift_start==self.shift_end: raise ValueError('shift_start must differ from shift_end')
        return self
class PunchInIn(BaseModel):
    model_config=ConfigDict(extra='forbid')
    emp_code:str; punched_at:Optional[int]=Field(default=None,ge=100000000000,le=4102444800000); status:Literal['PRESENT','WFH','ON_DUTY']='PRESENT'
    @field_validator('punched_at',mode='before')
    @classmethod
    def timestamp(cls,v):
        if v is not None and (isinstance(v,bool) or not isinstance(v,int)): raise ValueError('punched_at must be integer epoch milliseconds')
        return v
class PunchOutIn(BaseModel):
    model_config=ConfigDict(extra='forbid')
    emp_code:str; punched_at:Optional[int]=Field(default=None,ge=100000000000,le=4102444800000)
    @field_validator('punched_at',mode='before')
    @classmethod
    def timestamp(cls,v):
        if v is not None and (isinstance(v,bool) or not isinstance(v,int)): raise ValueError('punched_at must be integer epoch milliseconds')
        return v
class RegularizeIn(BaseModel):
    model_config=ConfigDict(extra='forbid')
    status:Optional[Literal['PRESENT','ABSENT','LEAVE','WFH','ON_DUTY']]=None; punch_in:Optional[int]=Field(default=None,ge=100000000000,le=4102444800000); punch_out:Optional[int]=Field(default=None,ge=100000000000,le=4102444800000); reason:str=Field(min_length=5,max_length=200); regularized_by:str=Field(min_length=1,max_length=50)
    @field_validator('punch_in','punch_out',mode='before')
    @classmethod
    def timestamps(cls,v):
        if v is not None and (isinstance(v,bool) or not isinstance(v,int)): raise ValueError('timestamps must be integer epoch milliseconds')
        return v

@app.get('/health')
def health():
    try: client.admin.command('ping'); return {'status':'ok'}
    except Exception: raise HTTPException(503,'MongoDB unavailable')
@app.post('/employees',status_code=201)
def create_employee(body:EmployeeIn):
    doc=body.model_dump(); doc['created_at']=now_utc()
    try: db.employees.insert_one(doc)
    except DuplicateKeyError: raise HTTPException(409,'emp_code already exists')
    doc['created_at']=to_ms(doc['created_at']); return doc
@app.get('/employees')
def list_employees(department:Optional[str]=None,page:int=Query(1,ge=1),page_size:int=Query(20,ge=1,le=100)):
    q={'department':department} if department else {}; total=db.employees.count_documents(q)
    items=list(db.employees.find(q,{'_id':0}).sort('emp_code',ASCENDING).skip((page-1)*page_size).limit(page_size))
    for x in items: x['created_at']=to_ms(x.get('created_at'))
    return {'items':items,'total':total,'page':page,'page_size':page_size}
@app.post('/attendance/punch-in',status_code=201)
def punch_in(body:PunchInIn):
    emp=db.employees.find_one({'emp_code':body.emp_code})
    if not emp: raise HTTPException(404,'employee not found')
    ts=from_ms(body.punched_at) if body.punched_at is not None else now_utc(); local=ist(ts); start,end=shift_bounds(ts,emp)
    # For overnight shifts, an early-morning punch belongs to the previous day's shift.
    att_date=local.date()
    if end.date()>start.date() and local.date()==end.date() and local.time()<time.fromisoformat(emp['shift_end']): att_date=start.date()
    d=att_date.isoformat()
    doc={'emp_code':body.emp_code,'date':d,'status':body.status,'punch_in':ts,'punch_out':None,'work_hours':None,'late_minutes':late_for(ts,emp),'overtime_minutes':0,'half_day':False,'history':[]}
    try: db.attendance_logs.insert_one(doc)
    except DuplicateKeyError: raise HTTPException(409,'already punched in for this date')
    return enrich(doc)
@app.post('/attendance/punch-out')
def punch_out(body:PunchOutIn):
    emp=db.employees.find_one({'emp_code':body.emp_code})
    if not emp: raise HTTPException(404,'employee not found')
    ts=from_ms(body.punched_at) if body.punched_at is not None else now_utc()
    doc=db.attendance_logs.find_one({'emp_code':body.emp_code,'punch_in':{'$lte':ts}},sort=[('punch_in',DESCENDING)])
    if not doc: raise HTTPException(404,'no punch-in found')
    if doc.get('punch_out') is not None: raise HTTPException(409,'already punched out')
    delta=(ts-as_utc(doc['punch_in'])).total_seconds()
    if delta<=0 or delta>86400: raise HTTPException(422,'punch-out must be after punch-in and within 24 hours')
    hours=work_hours(as_utc(doc['punch_in']),ts); overtime=overtime_for(ts,as_utc(doc['punch_in']),emp); half=hours<4.50
    updated=db.attendance_logs.find_one_and_update({'_id':doc['_id'],'punch_out':None},{'$set':{'punch_out':ts,'work_hours':hours,'overtime_minutes':overtime,'half_day':half}},return_document=ReturnDocument.AFTER)
    if not updated: raise HTTPException(409,'already punched out')
    return enrich(updated)
@app.get('/attendance')
def list_attendance(emp_code:Optional[str]=None,date_from:Optional[str]=None,date_to:Optional[str]=None,status:Optional[str]=None,page:int=Query(1,ge=1),page_size:int=Query(20,ge=1,le=100)):
    q=date_filter(date_from,date_to)
    if emp_code:q['emp_code']=emp_code
    if status:
        if status not in STATUSES: raise HTTPException(422,'invalid status')
        q['status']=status
    total=db.attendance_logs.count_documents(q); docs=list(db.attendance_logs.find(q).sort([('date',DESCENDING),('emp_code',ASCENDING)]).skip((page-1)*page_size).limit(page_size))
    return {'items':[enrich(x) for x in docs],'total':total,'page':page,'page_size':page_size}
@app.patch('/attendance/{emp_code}/{date}')
def regularize(emp_code:str,date:str,body:RegularizeIn):
    valid_date(date); emp=db.employees.find_one({'emp_code':emp_code})
    if not emp: raise HTTPException(404,'employee not found')
    doc=db.attendance_logs.find_one({'emp_code':emp_code,'date':date})
    if not doc: raise HTTPException(404,'attendance record not found')
    old={k:doc.get(k) for k in ('status','punch_in','punch_out','work_hours','late_minutes','overtime_minutes','half_day')}; new=dict(old)
    supplied = body.model_fields_set
    for k in ('status','punch_in','punch_out'):
        if k in supplied:
            v=getattr(body,k)
            new[k]=from_ms(v) if k in ('punch_in','punch_out') and v is not None else v
    if new['status'] in ('ABSENT','LEAVE'):
        if 'punch_in' in supplied or 'punch_out' in supplied: raise HTTPException(422,'ABSENT/LEAVE cannot have punch times')
        new['punch_in']=new['punch_out']=None
    elif new['punch_in'] is None: raise HTTPException(422,'presence status requires punch_in')
    if new['punch_in'] is not None and ist(new['punch_in']).date().isoformat()!=date: raise HTTPException(422,'punch_in must fall on attendance date')
    if new['punch_out'] is not None:
        sec=(as_utc(new['punch_out'])-as_utc(new['punch_in'])).total_seconds()
        if sec<=0 or sec>86400: raise HTTPException(422,'punch_out must be after punch_in and within 24 hours')
    new['late_minutes']=late_for(new['punch_in'],emp) if new['punch_in'] else 0
    new['work_hours']=work_hours(as_utc(new['punch_in']),as_utc(new['punch_out'])) if new['punch_in'] and new['punch_out'] else None
    new['overtime_minutes']=overtime_for(as_utc(new['punch_out']),as_utc(new['punch_in']),emp) if new['punch_in'] and new['punch_out'] else 0
    new['half_day']=bool(new['work_hours'] is not None and new['work_hours']<4.50)
    changes={}
    for k in old:
        a,b=old[k],new[k]
        if isinstance(a,datetime): a=as_utc(a)
        if isinstance(b,datetime): b=as_utc(b)
        if a!=b: changes[k]={'from':a,'to':b}
    if not changes: raise HTTPException(422,'request changes nothing')
    hist={'at':now_utc(),'by':body.regularized_by,'reason':body.reason,'changes':changes}
    # Atomic compare-and-swap prevents concurrent updates from losing history.
    updated=db.attendance_logs.find_one_and_update({'_id':doc['_id'],'history':doc.get('history',[])},{'$set':{k:new[k] for k in old},'$push':{'history':hist}},return_document=ReturnDocument.AFTER)
    if not updated: raise HTTPException(409,'record changed concurrently; retry')
    return enrich(updated)
@app.get('/analytics/employees/{emp_code}/monthly')
def employee_monthly(emp_code:str,month:str=Query(...,pattern=r'^\d{4}-(0[1-9]|1[0-2])$')):
    if not db.employees.find_one({'emp_code':emp_code}): raise HTTPException(404,'employee not found')
    valid_month(month); res=list(db.employees.aggregate(pipeline_employee_monthly(emp_code,month)))
    if not res: raise HTTPException(404,'employee not found')
    x=res[0]; return {'emp_code':emp_code,'month':month,'working_days':x.get('working_days',0),'present_days':round_half(x.get('present_days',0),2),'leave_days':x.get('leave_days',0),'late_count':x.get('late_count',0),'total_late_minutes':x.get('total_late_minutes',0),'total_overtime_minutes':x.get('total_overtime_minutes',0),'attendance_pct':round_half(x.get('attendance_pct'),2)}
@app.get('/analytics/departments/summary')
def department_summary(month:str=Query(...,pattern=r'^\d{4}-(0[1-9]|1[0-2])$'),department:Optional[str]=None):
    valid_month(month); rows=list(db.employees.aggregate(pipeline_department_summary(month,department)))
    for x in rows: x['present_days']=round_half(x.get('present_days',0),2); x['avg_work_hours']=round_half(x.get('avg_work_hours'),2)
    return {'month':month,'items':rows}
@app.get('/analytics/leaderboard/late')
def late_leaderboard(month:str=Query(...,pattern=r'^\d{4}-(0[1-9]|1[0-2])$'),limit:int=Query(10,ge=1,le=50),department:Optional[str]=None):
    valid_month(month); return {'month':month,'items':list(db.attendance_logs.aggregate(pipeline_late_leaderboard(month,department,limit)))}
@app.get('/analytics/departments/{department}/trend')
def department_trend(department:str,from_date:str=Query(...,alias='from'),to_date:str=Query(...,alias='to')):
    start=valid_date(from_date); end=valid_date(to_date)
    if end<start or (end-start).days+1>92: raise HTTPException(422,'invalid date range (maximum 92 days)')
    if db.employees.count_documents({'department':department})==0: raise HTTPException(404,'department not found')
    rows=list(db.employees.aggregate(pipeline_trend(department,from_date,to_date)))
    return {'department':department,'items':rows}

EXPLAIN_NAMES={'attendance_list','employee_monthly','department_summary','late_leaderboard','department_trend'}
@app.get('/admin/explain/{endpoint}')
def explain(endpoint:str,emp_code:Optional[str]=None,month:Optional[str]=None,department:Optional[str]=None,limit:int=Query(10,ge=1,le=50),date_from:Optional[str]=None,date_to:Optional[str]=None,status:Optional[str]=None,from_date:Optional[str]=Query(None,alias='from'),to_date:Optional[str]=Query(None,alias='to'),page:int=Query(1,ge=1),page_size:int=Query(20,ge=1,le=100)):
    if endpoint not in EXPLAIN_NAMES: raise HTTPException(422,'unknown endpoint')
    if endpoint=='attendance_list':
        q=date_filter(date_from,date_to)
        if emp_code:q['emp_code']=emp_code
        if status:q['status']=status
        cmd={'find':'attendance_logs','filter':q,'sort':{'date':-1,'emp_code':1},'skip':(page-1)*page_size,'limit':page_size}
        return db.command('explain',cmd,verbosity='executionStats')
    if not month: raise HTTPException(422,'month is required')
    valid_month(month)
    if endpoint=='employee_monthly':
        if not emp_code: raise HTTPException(422,'emp_code is required')
        coll=db.employees; pipe=pipeline_employee_monthly(emp_code,month)
    elif endpoint=='department_summary': coll=db.employees; pipe=pipeline_department_summary(month,department)
    elif endpoint=='late_leaderboard': coll=db.attendance_logs; pipe=pipeline_late_leaderboard(month,department,limit)
    else:
        if not department or not from_date or not to_date: raise HTTPException(422,'department, from and to are required')
        a=valid_date(from_date); b=valid_date(to_date)
        if b<a or (b-a).days+1>92: raise HTTPException(422,'invalid date range')
        coll=db.employees; pipe=pipeline_trend(department,from_date,to_date)
    return db.command('explain',{'aggregate':coll.name,'pipeline':pipe,'cursor':{}},verbosity='executionStats')
