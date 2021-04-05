/*Step 3: PULSE OX ANALYSIS for 2013-2017 VAPD (inpatient stays)*/
/*Author: Xiao Qing (Shirley) Wang*/
/*Date: 3/31/21*/

libname  final  'VAPD inpatient dataset location';
libname vital 'pulseox datset from step 2 cleaning';

/*Using the VA to VA transfer VAPDs*/
/*select only the variables wanted from VAPD 2014-2017 and VAPD 2013*/

/*VAPD 2014-2017 VA to VA transfer*/
DATA  VAPD_20142017  (compress=yes);  
retain patienticn sta6a  admityear datevalue new_admitdate3 new_dischargedate3 hosp_los  icu  female race age region new_teaching newhospcomm_sepsis
singlelevel_ccs chf pulm proccode_mechvent_daily  diuretics_daily inhosp_mort mort30_admit;
SET final.vatova20142017;
keep  patienticn sta6a  admityear datevalue new_admitdate3 new_dischargedate3 hosp_los  icu  female region new_teaching newhospcomm_sepsis race age
singlelevel_ccs chf pulm proccode_mechvent_daily diuretics_daily inhosp_mort mort30_admit
specialty specialtytransferdate specialtydischargedate;
RUN;

/*VAPD 2013 VA to VA transfer*/
DATA  VAPD_2013  (compress=yes);  
retain patienticn sta6a  admityear datevalue new_admitdate3 new_dischargedate3 hosp_los  icu  female race age region new_teaching newhospcomm_sepsis
singlelevel_ccs chf pulm proccode_mechvent_daily  diuretics_daily inhosp_mort mort30_admit;
SET final.vatova2013;
keep  patienticn sta6a  admityear datevalue new_admitdate3 new_dischargedate3 hosp_los  icu  female region new_teaching newhospcomm_sepsis race age
singlelevel_ccs chf pulm proccode_mechvent_daily diuretics_daily inhosp_mort mort30_admit specialty specialtytransferdate specialtydischargedate;
RUN;

/*combine VAPD 2014-3017 and VAPD 2013 datasets*/
DATA  VAPD_20132017  (compress=yes); 
SET  VAPD_20142017  VAPD_2013 ;
RUN;

/*assign each patienticn, newadmitdate3 & newdischargedate3 a unique hosp id*/;
/*create unique patient hosp count*/
PROC SORT DATA=VAPD_20132017  nodupkey  OUT=final_copy_undup2 (compress=yes keep=patienticn  new_admitdate3 new_dischargedate3 admityear); 
BY patienticn  new_admitdate3 new_dischargedate3;
RUN;

DATA final_copy_undup2 (compress=yes); 
SET final_copy_undup2 ;
unique_hosp=_N_; 
RUN;

/*match unique_hosp back to original dataset VAPD_20132017*/
PROC SQL;
	CREATE TABLE  VAPD_20132017_v2  (compress=yes)  AS   
	SELECT A.*, B.unique_hosp as unique_hosp_count_id
	FROM  VAPD_20132017  A
	LEFT JOIN final_copy_undup2  B ON A.patienticn=B.patienticn  and a.new_admitdate3=b.new_admitdate3 and a.new_dischargedate3=b.new_dischargedate3;
QUIT;

/*merge in PulseOX data: vital.PulseOx_YYYYMMDD*/
/*want to look at daily first, each patient day could have multiple pulseox values. */
PROC SQL;
	CREATE TABLE  VAPD_20132017_v3 (compress=yes)  AS /* 51805067 w/ duplicate pulseox values per patient-day*/
	SELECT A.*, B.vitalSignTakenDateTime, b.VitalResultNumeric, b.SupplementalO2, b.SpO2, b.O2_LPM
	FROM   VAPD_20132017_v2  A
	LEFT JOIN vital.PulseOx_YYYYMMDD  B
	ON A.patienticn=B.patienticn and a.datevalue=b.vital_date;
QUIT;

/*sort the data by unique_hosp_count_id, datevalue, and earliest vitalSignTakenDateTime*/
PROC SORT DATA=VAPD_20132017_v3 nodupkey OUT=VAPD_20132017_v3b (compress=yes); 
BY  unique_hosp_count_id datevalue vitalSignTakenDateTime SupplementalO2 SpO2 O2_LPM;
RUN;


/*Table 1: Pulse oximetry among VA hospitalizations, 2013-2017*/
/*Number of hospitalizations with patients who have 1 or more pulse ox measurements recorded, regardless of what their supplemental oxygen levels are.*/
/*give each obs a number=1*/
DATA VAPD_20132017_v3c (compress=yes); 
SET  VAPD_20132017_v3b;
if VitalResultNumeric NE . then pulseox_obs_ind=1; else pulseox_obs_ind=0;
if O2_LPM not in (0,.) then O2_LPM_obs_ind=1; else O2_LPM_obs_ind=0;
RUN;

PROC FREQ DATA=VAPD_20132017_v3c order=freq;
TABLE  pulseox_obs_ind O2_LPM_obs_ind;
RUN;

DATA hosp_count (compress=yes); 
SET VAPD_20132017_v3c;
if  pulseox_obs_ind=1;
RUN;

PROC SORT DATA=hosp_count  nodupkey; 
BY unique_hosp_count_id;
RUN;

PROC SORT DATA=hosp_count;
BY  admityear;
RUN;

PROC FREQ DATA= hosp_count  order=freq;
by admityear;
TABLE   pulseox_obs_ind;
RUN;


/*count number of pulseox & O2 readings by hosp and pat-days*/
PROC SQL;
CREATE TABLE VAPD_20132017_v3d (compress=yes)  AS 
SELECT *, sum(pulseox_obs_ind) as sum_PulseOX_count_hosp, sum(O2_LPM_obs_ind) as sum_O2_LPM_count_hosp
FROM VAPD_20132017_v3c
GROUP BY unique_hosp_count_id; /*count sum of pulseox & O2 check by each hospitalization*/
QUIT;

PROC SORT DATA= VAPD_20132017_v3d;
BY  unique_hosp_count_id  datevalue;
RUN;

/*Number of times pulseox & supplemental oxygen was checked in a DAY */
PROC SQL;
CREATE TABLE VAPD_20132017_v3e (compress=yes) AS 
SELECT *, sum(pulseox_obs_ind) as sum_PulseOX_count_daily,  sum(O2_LPM_obs_ind) as sum_O2_LPM_count_daily
FROM VAPD_20132017_v3d
GROUP BY unique_hosp_count_id, datevalue
order by unique_hosp_count_id, datevalue; /*count sum of pulseox check by each patient-day*/
QUIT;


/*get unique hosp and then get mean, median, IQRs*/
PROC SORT DATA=VAPD_20132017_v3e  nodupkey  OUT=tab1_hosp (compress=yes keep=admityear unique_hosp_count_id sum_PulseOX_count_hosp sum_O2_LPM_count_hosp);
BY  unique_hosp_count_id;
RUN;

PROC SORT DATA=tab1_hosp;
BY admityear ;
RUN;

/*delete the 0 readings per hosp*/
DATA tab1_hosp (compress=yes);
SET  tab1_hosp;
if sum_PulseOX_count_hosp =0 then delete;
RUN;

PROC MEANS DATA=tab1_hosp   MIN MAX MEAN MEDIAN Q1 Q3;
VAR sum_PulseOX_count_hosp;
RUN;

PROC MEANS DATA=tab1_hosp  MIN MAX MEAN MEDIAN Q1 Q3;
by admityear;
VAR sum_PulseOX_count_hosp;
RUN;

/*get unique unique pat-days and then get mean, median, IQRs*/
PROC SORT DATA=VAPD_20132017_v3e  nodupkey  OUT=tab1_days (compress=yes keep=admityear unique_hosp_count_id datevalue sum_PulseOX_count_daily sum_O2_LPM_count_daily);
BY  unique_hosp_count_id datevalue;
RUN;

PROC SORT DATA=tab1_days;
BY admityear;
RUN;

/*delete the 0 readings per day*/
DATA tab1_days (compress=yes); 
SET  tab1_days;
if sum_PulseOX_count_daily <1 then delete;
RUN;

PROC MEANS DATA=tab1_days  MIN MAX MEAN MEDIAN Q1 Q3;
VAR sum_PulseOX_count_daily;
RUN;

PROC MEANS DATA=tab1_days MIN MAX MEAN MEDIAN Q1 Q3;
by admityear;
VAR sum_PulseOX_count_daily ;
RUN;

/*Number of pulse oximetry measurements per day, N(%)  by admityear*/
PROC FREQ DATA=tab1_days ;
/*where admityear=2017;*/ /*change years*/
TABLE sum_PulseOX_count_daily ;
RUN;

/*Hours  between pulse oximetry measurements within a hospitalization */
/*some pat-days have no pulseox, delete those*/
DATA tab1_duration (compress=yes) ;
SET  VAPD_20132017_v3e;
if vitalSignTakenDateTime =. then delete;
RUN;

/*first, sort by unique_hosp_count_id & vitalSignTakenDateTime*/
PROC SORT DATA=tab1_duration nodupkey  OUT=tab1_duration2 (compress=yes keep=patienticn unique_hosp_count_id admityear vitalSignTakenDateTime VitalResultNumeric pulseox_obs_ind);
BY unique_hosp_count_id  vitalSignTakenDateTime; 
RUN;

/*count number of pulseox_obs_ind by hosp, delete <=1*/
PROC SQL;
CREATE TABLE tab1_durationb (compress=yes)  AS  
SELECT *, sum(pulseox_obs_ind) as sum_PulseOX_count_hosp
FROM tab1_duration2
GROUP BY unique_hosp_count_id; /*count sum of pulseox check by each hospitalization*/
QUIT;

DATA tab1_durationc (compress=yes); 
SET  tab1_durationb;
if sum_PulseOX_count_hosp <2 then delete;
RUN;

DATA  tab1_durationd  (compress=yes);
SET  tab1_durationc;
by unique_hosp_count_id;
if first.unique_hosp_count_id  then do;
lag_pulseoxtime=vitalSignTakenDateTime; end;
lag_pulseoxtime2=lag(vitalSignTakenDateTime);
format lag_pulseoxtime datetime20.  lag_pulseoxtime2 datetime20.;
RUN;

DATA  tab1_duratione  (compress=yes);
SET  tab1_durationd;
if lag_pulseoxtime NE '' then lag_pulseoxtime2= '';
if lag_pulseoxtime = '' then lag_pulseoxtime=lag_pulseoxtime2;
drop lag_pulseoxtime2 ;
by unique_hosp_count_id;
if first.unique_hosp_count_id then do lag_pulseoxtime=.; end;
RUN;

DATA tab1_duratione  (compress=yes); 
SET tab1_duratione; 
if lag_pulseoxtime =. then hour_diff=.;
 else hour_diff = INTCK('hour',lag_pulseoxtime,vitalSignTakenDateTime);
if lag_pulseoxtime =. then hour_diff=.;
else minute_diff=INTCK('minute',lag_pulseoxtime,vitalSignTakenDateTime);
RUN;

PROC MEANS DATA=tab1_duratione  mean MEDIAN Q1 Q3;
class admityear;
VAR  hour_diff minute_diff;
RUN;

PROC MEANS DATA=tab1_duratione  mean MEDIAN Q1 Q3;
VAR  hour_diff minute_diff;
RUN;


/*Duration of hospitalization with pulse oximetry monitoring*/
/*First, we would want to find the difference in hours between the first and last pulse ox reading for a given hospitalization. */
DATA tab1_duration_last (compress=yes);
SET VAPD_20132017_V3E;
if vitalSignTakenDateTime =. then delete;
hosp_los_hr=hosp_los*24; /*Second, we will create a new variable that calculates the LOS (in hours) for each hospitalization.*/
keep patienticn unique_hosp_count_id admityear vitalSignTakenDateTime hosp_los hosp_los_hr;
RUN;

PROC SORT DATA=tab1_duration_last   OUT=first (keep= patienticn unique_hosp_count_id admityear vitalSignTakenDateTime compress=yes);
BY unique_hosp_count_id vitalSignTakenDateTime;
RUN;

DATA first_v2 (compress=yes);
SET  first;
by unique_hosp_count_id;
if first.unique_hosp_count_id  then do;
keep=1; end;
if keep=1;
drop keep;
RUN;

PROC SORT DATA=tab1_duration_last   OUT=last (keep= patienticn unique_hosp_count_id admityear vitalSignTakenDateTime compress=yes);
BY unique_hosp_count_id descending vitalSignTakenDateTime;
RUN;

DATA last_v2 (compress=yes);
SET  last;
by unique_hosp_count_id;
if first.unique_hosp_count_id  then do;
keep=1; end;
if keep=1;
drop keep;
RUN;

PROC SORT DATA=tab1_duration_last  nodupkey ;
BY unique_hosp_count_id ;
RUN;

PROC SQL;
	CREATE TABLE tab1_duration_last_v2  (compress=yes)  AS
	SELECT A.*, B.vitalSignTakenDateTime as first_vital_hosp, c.vitalSignTakenDateTime as last_vital_hosp
	FROM tab1_duration_last    A
	LEFT JOIN first_v2  B ON A.unique_hosp_count_id =B.unique_hosp_count_id
     LEFT JOIN last_v2  C ON A.unique_hosp_count_id =C.unique_hosp_count_id;
QUIT;

/*Third, we will find the proportion of each hospitalization that has had pulse ox monitoring by dividing #1 by #2 (above). */
DATA tab1_duration_last_v3 (compress=yes) ;
SET  tab1_duration_last_v2;
hour_diff = INTCK('hour',first_vital_hosp,last_vital_hosp);
if hour_diff=0 then delete;
hosp_diff=hour_diff/hosp_los_hr;
RUN;

PROC MEANS DATA=tab1_duration_last_v3  mean MEDIAN Q1 Q3;
class admityear;
VAR  hosp_diff ;
RUN;

PROC MEANS DATA=tab1_duration_last_v3 mean MEDIAN Q1 Q3;
VAR  hosp_diff ;
RUN;

/*additional analysis from Jack's manuscript revision. 1/19/21*/
/*The VAPD consists of a total of 15,437,270 patient-days for 2,765,446 hospitalizations at 134 VA 
hospitals from 2013-2017. Among all hospitalizations, 2,700,922 (97.7%) had at least one pulse oximetry 
reading and 864,605 (31%) received oxygen therapy. Data were present for XXX (XX%) of days not in an ICU, 
and XX (XX%) of days on which there were transitions into an ICU, and XX (XX%) of other ICU days.*/

DATA not_ICU_days (compress=yes); 
SET VAPD_20132017_v3c;
if icu=0 and pulseox_obs_ind=1;
RUN;

PROC SORT DATA=not_ICU_days  nodupkey  OUT=uniquenot_ICU_days (compress=yes);  
BY  patienticn datevalue;
RUN;

DATA ICU_days (compress=yes); 
SET VAPD_20132017_v3c;
if icu=1 and pulseox_obs_ind=1;
if icu=1 and (specialtytransferdate=datevalue) then icu_day1_ind=1; else icu_day1_ind=0;
RUN;

DATA ICU_day1 (compress=yes); 
SET  ICU_days ;
if icu_day1_ind=1;
RUN;

PROC SORT DATA=ICU_day1  nodupkey  OUT=unique_ICU_day1 (compress=yes);  
BY  patienticn datevalue;
RUN;

DATA notICU_day1 (compress=yes); 
SET  ICU_days ;
if icu_day1_ind=0;
RUN;

PROC SORT DATA=notICU_day1  nodupkey  OUT=unique_notICU_day1 (compress=yes);  
BY  patienticn datevalue;
RUN;

/* histogram of Supplemental Oxygen Rates among those with at least some Oxygen*/
DATA hist_oxygen (compress=yes); /*5974321*/
SET VAPD_20132017_v3c;
if O2_LPM_obs_ind=1;
RUN;

proc sgplot data=hist_oxygen noautolegend;
 histogram O2_LPM;
run;

/*Redo Figure 2 lumping everyone on greater than or equal to 15 L into a single bar labelled 15+, otherwise as is*/
DATA hist_oxygen_v2 (compress=yes);
SET hist_oxygen;
if O2_LPM >=15 then new_O2_LPM='15+'; else new_O2_LPM=O2_LPM;
if O2_LPM >=15 then new_O2_LPM_num=15; else new_O2_LPM_num=O2_LPM;
RUN;

PROC FREQ DATA=hist_oxygen_v2; /*only 1% that's 15+*/
TABLE new_O2_LPM;
RUN;


proc sgplot data=hist_oxygen_v2;
histogram new_O2_LPM_num;
run;

proc sgplot data=hist_oxygen_v2;
vbar new_O2_LPM_num;
run;


/*The distribution of pulse oximetry readings is shown in Figure X, with XX% reading at 88% or higher.*/
DATA pulseox_only (compress=yes);
SET VAPD_20132017_v3c;
if pulseox_obs_ind=1;
if SpO2 >=88 then count=1; else count=0;
RUN;

PROC FREQ DATA=pulseox_only  order=freq;
TABLE  count;
RUN;

proc sgplot data=pulseox_only noautolegend;
 histogram SpO2;
run;


/*Predictive Validity: Are higher levels of supplemental oxygen use on the first day of hospitalization associated with higher rates of inpatient mortality?*/
/*create a data set of day 1 of hospitalization*/
DATA day1_only (compress=yes); 
SET VAPD_20132017_v3c;
if datevalue=new_admitdate3;
if O2_LPM=. then O2_LPM=0;
RUN;

PROC FREQ DATA=day1_only  order=freq;
TABLE O2_LPM ;
RUN;

PROC SORT DATA=day1_only;
BY patienticn datevalue descending  O2_LPM;
RUN;

PROC SORT DATA= day1_only nodupkey  OUT=day1_only_v2 (compress=yes); 
BY  patienticn datevalue;
RUN;

/*UNADJUSTED LOGISTIC REGRESSION ANALYSIS*/
proc logistic data=day1_only_v2;
model inhosp_mort (event='1')= O2_LPM / RSQ EXPB CL;
run;



/*Are higher levels of supplemental oxygen on the last of the hospitalization associated with higher rates of mortality in the subsequent year?*/
/*create a data set of last day of hospitalization*/
DATA lastday_only (compress=yes);
SET VAPD_20132017_v3c;
if datevalue=new_dischargedate3;
if O2_LPM=. then O2_LPM=0;
RUN;

PROC SORT DATA=lastday_only;
BY patienticn datevalue descending  O2_LPM;
RUN;

PROC SORT DATA= lastday_only nodupkey  OUT=lastday_only_v2 (compress=yes); 
BY  patienticn datevalue;
RUN;

/*Need Date of Death data for each patient*/
/*create mort365_discharge variable, my merging with dod data 2013-2018*/
libname dod 'Date of Death dataset location';

PROC SQL;
	CREATE TABLE lastday_only_v3  (compress=yes)  AS
	SELECT A.*, B.dod_20210112_pull
	FROM  lastday_only_v2   A
	LEFT JOIN dod.DOD_20210112_PULL  B ON A.patienticn=B.patienticn;
QUIT;

/*recalculate 30 day mort and in hosp mort*/
DATA lastday_only_v4 (compress=yes);  
SET lastday_only_v3;
/*365 day mort after discharge*/
if not missing(DOD_20210112_PULL) then do; 
	deathdaysafterdis=datdif(new_dischargedate3,DOD_20210112_PULL, 'act/act');
end;
if not missing(DOD_20210112_PULL) and abs(deathdaysafterdis) <=365 then mort365_discharge=1;
       else mort365_discharge=0;
RUN;

/*UNADJUSTED LOGISTIC REGRESSION ANALYSIS*/
proc logistic data=lastday_only_v4;
model mort365_discharge  (event='1')= O2_LPM / RSQ EXPB CL;
run;



/***** TABLE 2: Pulse oximetry for patients receiving supplemental oxygen (SO) among VA hospitalizations, 2013-2017 *****/
/*create new variables*/
/*Hospitalizations w/ patients ever on supplemental oxygen*/
DATA PulseOx_hosp_ind   (compress=yes); 
SET  VAPD_20132017_v3e;
if O2_LPM_obs_ind=1;
RUN;

PROC SORT DATA=PulseOx_hosp_ind out=hosp_count_v2  nodupkey; 
BY unique_hosp_count_id;
RUN;

PROC SORT DATA=hosp_count_v2;
BY  admityear;
RUN;

PROC FREQ DATA=hosp_count_v2  order=freq;
/*by admityear;*/
TABLE  O2_LPM_obs_ind;
RUN;


/*SO hospitalizations with pulse oximetry?*/
PROC FREQ DATA=PulseOx_hosp_ind   order=freq; /*all have pulseox readings*/
TABLE pulseox_obs_ind;
RUN;


/*Number of pulse oximetry measurements during SO hospitalizations only*/
/*select only SO hosps and then look at Number of PulseOx readings*/
DATA SO_hosp_only (compress=yes);
SET VAPD_20132017_v3e;
if sum_O2_LPM_count_hosp >0; /*select only those with sum O2 LPM more than 0 hospitalizations*/
RUN;

PROC SORT DATA=SO_hosp_only  nodupkey  OUT=tab2_hosp (compress=yes keep=admityear unique_hosp_count_id sum_PulseOX_count_hosp sum_O2_LPM_count_hosp);
BY  unique_hosp_count_id;
RUN;

PROC SORT DATA=tab2_hosp;
BY admityear;
RUN;

/*delete the 0 readings per hosp*/
DATA tab2_hosp (compress=yes); 
SET  tab2_hosp;
if sum_PulseOX_count_hosp =0 then delete;
RUN;

PROC MEANS DATA=tab2_hosp   MIN MAX MEAN MEDIAN Q1 Q3;
VAR sum_PulseOX_count_hosp;
RUN;

PROC MEANS DATA=tab2_hosp  MIN MAX MEAN MEDIAN Q1 Q3;
by admityear;
VAR sum_PulseOX_count_hosp;
RUN;

/*get unique unique pat-days and then get mean, median, IQRs*/
PROC SORT DATA=SO_hosp_only  nodupkey  OUT=tab2_days (compress=yes keep=admityear unique_hosp_count_id datevalue sum_PulseOX_count_daily sum_O2_LPM_count_daily);
BY  unique_hosp_count_id datevalue;
RUN;

PROC SORT DATA=tab2_days;
BY admityear;
RUN;

/*delete the 0 readings per day*/
DATA tab2_days (compress=yes); 
SET  tab2_days;
if sum_PulseOX_count_daily <1 then delete;
RUN;

PROC MEANS DATA=tab2_days  MIN MAX MEAN MEDIAN Q1 Q3;
VAR sum_PulseOX_count_daily;
RUN;

PROC MEANS DATA=tab2_days MIN MAX MEAN MEDIAN Q1 Q3;
by admityear;
VAR sum_PulseOX_count_daily;
RUN;

/*Number of pulse oximetry measurements per day, N(%)  by admityear*/
PROC FREQ DATA=tab2_days;
where admityear=2017; /*change years*/
TABLE sum_PulseOX_count_daily;
RUN;


/*Hours  between pulse oximetry measurements within a hospitalization */
/*some pat-days have no pulseox, delete those*/
DATA tab2_duration (compress=yes);
SET  SO_hosp_only;
if vitalSignTakenDateTime =. then delete;
RUN;

/*first, sort by unique_hosp_count_id & vitalSignTakenDateTime*/
PROC SORT DATA=tab2_duration nodupkey  OUT=tab2_duration2 (compress=yes keep=patienticn unique_hosp_count_id admityear vitalSignTakenDateTime VitalResultNumeric pulseox_obs_ind);
BY unique_hosp_count_id  vitalSignTakenDateTime; 
RUN;

/*count number of pulseox_obs_ind by hosp, delete <=1*/
PROC SQL;
CREATE TABLE tab2_durationb (compress=yes)  AS  
SELECT *, sum(pulseox_obs_ind) as sum_PulseOX_count_hosp
FROM tab2_duration2
GROUP BY unique_hosp_count_id; /*count sum of pulseox check by each hospitalization*/
QUIT;

DATA tab2_durationc (compress=yes);
SET  tab2_durationb;
if sum_PulseOX_count_hosp <2 then delete;
RUN;

DATA  tab2_durationd  (compress=yes);
SET  tab2_durationc;
by unique_hosp_count_id;
if first.unique_hosp_count_id  then do;
lag_pulseoxtime=vitalSignTakenDateTime; end;
lag_pulseoxtime2=lag(vitalSignTakenDateTime);
format lag_pulseoxtime datetime20.  lag_pulseoxtime2 datetime20.;
RUN;

DATA  tab2_duratione  (compress=yes);
SET  tab2_durationd;
if lag_pulseoxtime NE '' then lag_pulseoxtime2= '';
if lag_pulseoxtime = '' then lag_pulseoxtime=lag_pulseoxtime2;
drop lag_pulseoxtime2 ;
by unique_hosp_count_id;
if first.unique_hosp_count_id then do lag_pulseoxtime=.; end;
RUN;

DATA tab2_duratione  (compress=yes); 
SET tab2_duratione; 
if lag_pulseoxtime =. then hour_diff=.;
 else hour_diff = INTCK('hour',lag_pulseoxtime,vitalSignTakenDateTime);
if lag_pulseoxtime =. then hour_diff=.;
else minute_diff=INTCK('minute',lag_pulseoxtime,vitalSignTakenDateTime);
/*if hour_diff=. then delete;*/
RUN;

PROC MEANS DATA=tab2_duratione  mean MEDIAN Q1 Q3;
class admityear;
VAR  hour_diff minute_diff;
RUN;

PROC MEANS DATA=tab2_duratione  mean MEDIAN Q1 Q3;
VAR  hour_diff minute_diff;
RUN;


/*Duration of hospitalization with pulse oximetry monitoring*/
/*First, we would want to find the difference in hours between the first and last pulse ox reading for a given hospitalization. */
DATA tab2_duration_last (compress=yes);
SET SO_hosp_only;
if vitalSignTakenDateTime =. then delete;
hosp_los_hr=hosp_los*24; /*Second, we will create a new variable that calculates the LOS (in hours) for each hospitalization.*/
keep patienticn unique_hosp_count_id admityear vitalSignTakenDateTime hosp_los hosp_los_hr;
RUN;

PROC SORT DATA=tab2_duration_last   OUT=first (keep= patienticn unique_hosp_count_id admityear vitalSignTakenDateTime compress=yes);
BY unique_hosp_count_id vitalSignTakenDateTime;
RUN;

DATA first_v2 (compress=yes);
SET  first;
by unique_hosp_count_id;
if first.unique_hosp_count_id  then do;
keep=1; end;
if keep=1;
drop keep;
RUN;

PROC SORT DATA=tab2_duration_last   OUT=last (keep= patienticn unique_hosp_count_id admityear vitalSignTakenDateTime compress=yes);
BY unique_hosp_count_id descending vitalSignTakenDateTime;
RUN;

DATA last_v2 (compress=yes);
SET  last;
by unique_hosp_count_id;
if first.unique_hosp_count_id  then do;
keep=1; end;
if keep=1;
drop keep;
RUN;

PROC SORT DATA=tab2_duration_last  nodupkey;
BY unique_hosp_count_id;
RUN;

PROC SQL;
	CREATE TABLE tab2_duration_last_v2  (compress=yes)  AS
	SELECT A.*, B.vitalSignTakenDateTime as first_vital_hosp, c.vitalSignTakenDateTime as last_vital_hosp
	FROM tab2_duration_last  A
	LEFT JOIN first_v2  B ON A.unique_hosp_count_id =B.unique_hosp_count_id
     LEFT JOIN last_v2  C ON A.unique_hosp_count_id =C.unique_hosp_count_id;
QUIT;

/*Third, we will find the proportion of each hospitalization that has had pulse ox monitoring by dividing #1 by #2 (above). */
DATA tab2_duration_last_v3 (compress=yes);
SET  tab2_duration_last_v2;
hour_diff = INTCK('hour',first_vital_hosp,last_vital_hosp);
if hour_diff=0 then delete;
hosp_diff=hour_diff/hosp_los_hr;
RUN;

PROC MEANS DATA=tab2_duration_last_v3  mean MEDIAN Q1 Q3;
class admityear;
VAR  hosp_diff;
RUN;

PROC MEANS DATA=tab2_duration_last_v3 mean MEDIAN Q1 Q3;
VAR  hosp_diff;
RUN;

/***************************************************************************************************/
/*3/24/21: additional analysis for Table 2 revision*/
/*cohort: Receiving Pulse Oximetry but not supplemental oxygen (table 1 cohort - table 2 cohort)*/
/*rerun table 1 analysis on this new cohort*/
PROC SQL;
CREATE TABLE  only_pulseox_cohort_hosp   (COMPRESS=YES) AS 
SELECT A.* FROM tab1_hosp AS A
WHERE A.unique_hosp_count_id not IN (SELECT  unique_hosp_count_id  FROM tab2_duration_last_v2);
QUIT;

PROC FREQ DATA=only_pulseox_cohort_hosp  order=freq; /*yes, all 0*/
TABLE  sum_O2_LPM_count_hosp;
RUN;


/*Number of pulse oximetry measurements during hospitalizations, if at least one*/
PROC MEANS DATA=only_pulseox_cohort_hosp   MIN MAX MEAN MEDIAN Q1 Q3;
VAR sum_PulseOX_count_hosp;
RUN;

/*Number of pulse oximetry measurements per day, if at least one*/
PROC SQL;
CREATE TABLE  add_tab2_days  (COMPRESS=YES) AS 
SELECT A.* FROM tab1_days AS A
WHERE A.unique_hosp_count_id IN (SELECT unique_hosp_count_id  FROM only_pulseox_cohort_hosp);
QUIT;

PROC SORT DATA=add_tab2_days  nodupkey  OUT=test;  
BY unique_hosp_count_id;
RUN;

PROC MEANS DATA=add_tab2_days  MIN MAX MEAN MEDIAN Q1 Q3;
VAR sum_PulseOX_count_daily;
RUN;


/*Number of pulse oximetry measurements per calendar day, if at least one N(%) */
PROC FREQ DATA=add_tab2_days;
TABLE sum_PulseOX_count_daily;
RUN;

/*Hours between pulse oximetry measurements within a hospitalization */
PROC SQL;
CREATE TABLE  add_tab2_duratione (COMPRESS=YES) AS 
SELECT A.* FROM tab1_duratione AS A
WHERE A.unique_hosp_count_id IN (SELECT unique_hosp_count_id FROM only_pulseox_cohort_hosp);
QUIT;

PROC SORT DATA=add_tab2_duratione  nodupkey  OUT=test;  /*1757004, some hosp with only 1 pulseox, hence no hour duration.*/
BY unique_hosp_count_id;
RUN;

PROC MEANS DATA=add_tab2_duratione mean MEDIAN Q1 Q3;
VAR  hour_diff minute_diff;
RUN;

/*Proportion/Duration of hospitalization with pulse oximetry monitoring*/
PROC SQL;
CREATE TABLE  add_tab2_duration_last_v3 (COMPRESS=YES) AS 
SELECT A.* FROM tab1_duration_last_v3 AS A
WHERE A.unique_hosp_count_id IN (SELECT unique_hosp_count_id FROM only_pulseox_cohort_hosp);
QUIT;

PROC SORT DATA=add_tab2_duration_last_v3  nodupkey  OUT=test;  /*1754597, some hosp with only 1 pulseox, hence no hour duration.*/
BY unique_hosp_count_id;
RUN;

PROC MEANS DATA=add_tab2_duration_last_v3 mean MEDIAN Q1 Q3;
VAR  hosp_diff;
RUN;




/***************************************************************************************************/
/*Table 3: got SO vs Didn't*/
/***************************************************************************************************/
/*Need hosp-level data for Table 3: Patient characteristics*/
/*create ICU_hosp, Mechanical ventilation_hosp, On any diuretics_hosp, CDC Sepsis_hosp variables.
Comorbidities, med (IQR): Heart failure (CHF), N(%), Chronic pulmonary disease, N(%),
Top 20 single-level CCS diagnoses:  Congestive heart failure; non-hypertensive, Nonspecific chest pain, Coronary atherosclerosis and other heart disease, Cardiac dysrhythmias, Alcohol-related disorders,
Septicemia (except in labor),Chronic obstructive pulmonary disease and bronchiectasis,Pneumonia ,Skin and subcutaneous tissue infections,Osteoarthritis,Complication of device; implant or graft,
Complications of surgical procedures or medical care, Diabetes mellitus with complications, Respiratory failure; insufficiency; arrest (adult), Urinary tract infections, Acute and unspecified renal failure,
Spondylosis; intervertebral disc disorders; other back problems, Acute myocardial infarction, Fluid and electrolyte disorders, Gastrointestinal hemorrhage*/ 

/*ICU_hosp*/
DATA  ICU_hosp (compress=yes); 
SET  VAPD_20132017_v2;
if icu=1;
keep unique_hosp_count_id icu;
RUN;

PROC SORT DATA= ICU_hosp  nodupkey;
BY unique_hosp_count_id icu;
RUN;

/*Mechanical ventilation_hosp*/
DATA  mechvent_hosp (compress=yes); 
SET  VAPD_20132017_v2;
if proccode_mechvent_daily=1;
proccode_mechvent_hosp=1;
keep unique_hosp_count_id proccode_mechvent_daily proccode_mechvent_hosp;
RUN;

PROC SORT DATA=mechvent_hosp  nodupkey; 
BY unique_hosp_count_id proccode_mechvent_hosp;
RUN;

/*On any diuretics_hosp*/
DATA  diuretics_hosp (compress=yes); 
SET  VAPD_20132017_v2 ;
if diuretics_daily=1;
diuretics_hosp=1;
keep unique_hosp_count_id diuretics_daily diuretics_hosp;
RUN;

PROC SORT DATA=diuretics_hosp  nodupkey; 
BY unique_hosp_count_id diuretics_hosp;
RUN;

/*CDC Sepsis_hosp*/
DATA  Sepsis_hosp  (compress=yes); 
SET  VAPD_20132017_v2 ;
if newhospcomm_sepsis=1;
Sepsis_hosp=1;
keep unique_hosp_count_id newhospcomm_sepsis Sepsis_hosp;
RUN;

PROC SORT DATA=Sepsis_hosp  nodupkey; 
BY unique_hosp_count_id Sepsis_hosp;
RUN;

/*comorbid: Heart failure (CHF)*/
DATA  CHF_hosp  (compress=yes); 
SET  VAPD_20132017_v2 ;
if chf=1;
CHF_hosp=1;
keep unique_hosp_count_id chf CHF_hosp;
RUN;

PROC SORT DATA=CHF_hosp  nodupkey; 
BY unique_hosp_count_id CHF_hosp;
RUN;

/*comorbid:chronic pulmonary disease*/
DATA  pulm_hosp  (compress=yes); 
SET  VAPD_20132017_v2;
if pulm=1;
pulm_hosp=1;
keep unique_hosp_count_id pulm pulm_hosp;
RUN;

PROC SORT DATA=pulm_hosp  nodupkey; 
BY unique_hosp_count_id pulm_hosp;
RUN;

/*Top 20 single-level CCS diagnoses:  Congestive heart failure; non-hypertensive, Nonspecific chest pain, Coronary atherosclerosis and other heart disease, Cardiac dysrhythmias, Alcohol-related disorders,
Septicemia (except in labor),Chronic obstructive pulmonary disease and bronchiectasis,Pneumonia ,Skin and subcutaneous tissue infections,Osteoarthritis,Complication of device; implant or graft,
Complications of surgical procedures or medical care, Diabetes mellitus with complications, Respiratory failure; insufficiency; arrest (adult), Urinary tract infections, Acute and unspecified renal failure,
Spondylosis; intervertebral disc disorders; other back problems, Acute myocardial infarction, Fluid and electrolyte disorders, Gastrointestinal hemorrhage*/
DATA  ccs_chf  ccs_Chestpain ccs_atherosclerosis ccs_Cardiac ccs_Alcohol ccs_Septicemia ccs_COPD ccs_Pneumonia ccs_Skin  ccs_Osteoarthritis;
SET  VAPD_20132017_v2;
if singlelevel_ccs=108 then output   ccs_chf; 
if singlelevel_ccs=102 then output   ccs_Chestpain; 
if singlelevel_ccs=101 then output   ccs_atherosclerosis; 
if singlelevel_ccs=106 then output   ccs_Cardiac; 
if singlelevel_ccs=660 then output   ccs_Alcohol; 
if singlelevel_ccs=2   then output   ccs_Septicemia; 
if singlelevel_ccs=127 then output   ccs_COPD; 
if singlelevel_ccs=122 then output   ccs_Pneumonia; 
if singlelevel_ccs=197 then output   ccs_Skin;
if singlelevel_ccs=203 then output   ccs_Osteoarthritis; 
RUN;


DATA  ccs_CompDevice  ccs_CompSurge  ccs_Diabetes ccs_Respiratory ccs_UTI ccs_renal  ccs_Spondylosis ccs_AMI ccs_FED  ccs_Gas;
SET   VAPD_20132017_v2;
if singlelevel_ccs=237  then output   ccs_CompDevice; 
if singlelevel_ccs=238  then output   ccs_CompSurge;
if singlelevel_ccs=50   then output   ccs_Diabetes;
if singlelevel_ccs=131  then output   ccs_Respiratory;
if singlelevel_ccs=159  then output   ccs_UTI;
if singlelevel_ccs=157  then output   ccs_renal; 
if singlelevel_ccs=205  then output   ccs_Spondylosis;
if singlelevel_ccs=100  then output   ccs_AMI;
if singlelevel_ccs= 55  then output   ccs_FED;
if singlelevel_ccs=153  then output   ccs_Gas;
RUN;

/*ccs_chf*/
DATA  ccs_chf (compress=yes); 
SET  ccs_chf ;
ccs_chf_hosp=1;
keep unique_hosp_count_id singlelevel_ccs ccs_chf_hosp;
RUN;

PROC SORT DATA=ccs_chf  nodupkey; 
BY unique_hosp_count_id ccs_chf_hosp;
RUN;

/*ccs_alcohol*/
DATA  ccs_alcohol (compress=yes); 
SET  ccs_alcohol ;
ccs_alcohol_hosp=1;
keep unique_hosp_count_id singlelevel_ccs ccs_alcohol_hosp;
RUN;

PROC SORT DATA=ccs_alcohol  nodupkey; 
BY unique_hosp_count_id ccs_alcohol_hosp;
RUN;

/*ccs_ami*/
DATA  ccs_ami (compress=yes); 
SET  ccs_ami ;
ccs_ami_hosp=1;
keep unique_hosp_count_id singlelevel_ccs ccs_ami_hosp;
RUN;

PROC SORT DATA=ccs_ami  nodupkey; 
BY unique_hosp_count_id ccs_ami_hosp;
RUN;

/*ccs_atherosclerosis*/
DATA  ccs_atherosclerosis (compress=yes); 
SET  ccs_atherosclerosis ;
ccs_atherosclerosis_hosp=1;
keep unique_hosp_count_id singlelevel_ccs ccs_atherosclerosis_hosp;
RUN;

PROC SORT DATA=ccs_atherosclerosis  nodupkey; 
BY unique_hosp_count_id ccs_atherosclerosis_hosp;
RUN;

/*ccs_cardiac*/
DATA  ccs_cardiac (compress=yes); 
SET  ccs_cardiac ;
ccs_cardiac_hosp=1;
keep unique_hosp_count_id singlelevel_ccs ccs_cardiac_hosp;
RUN;

PROC SORT DATA=ccs_cardiac  nodupkey; 
BY unique_hosp_count_id ccs_cardiac_hosp;
RUN;

/*ccs_chestpain*/
DATA  ccs_chestpain (compress=yes);
SET  ccs_chestpain ;
ccs_chestpain_hosp=1;
keep unique_hosp_count_id singlelevel_ccs ccs_chestpain_hosp;
RUN;

PROC SORT DATA=ccs_chestpain  nodupkey; 
BY unique_hosp_count_id ccs_chestpain_hosp;
RUN;

/*ccs_compdevice*/
DATA  ccs_compdevice (compress=yes); 
SET  ccs_compdevice ;
ccs_compdevice_hosp=1;
keep unique_hosp_count_id singlelevel_ccs ccs_compdevice_hosp;
RUN;

PROC SORT DATA=ccs_compdevice  nodupkey; 
BY unique_hosp_count_id ccs_compdevice_hosp;
RUN;

/*ccs_compsurge*/
DATA  ccs_compsurge (compress=yes); 
SET  ccs_compsurge ;
ccs_compsurge_hosp=1;
keep unique_hosp_count_id singlelevel_ccs ccs_compsurge_hosp;
RUN;

PROC SORT DATA=ccs_compsurge  nodupkey; 
BY unique_hosp_count_id ccs_compsurge_hosp;
RUN;

/*ccs_copd*/
DATA  ccs_copd (compress=yes); 
SET  ccs_copd ;
ccs_copd_hosp=1;
keep unique_hosp_count_id singlelevel_ccs ccs_copd_hosp;
RUN;

PROC SORT DATA=ccs_copd  nodupkey; 
BY unique_hosp_count_id ccs_copd_hosp;
RUN;

/*ccs_diabetes*/
DATA  ccs_diabetes (compress=yes); 
SET  ccs_diabetes ;
ccs_diabetes_hosp=1;
keep unique_hosp_count_id singlelevel_ccs ccs_diabetes_hosp;
RUN;

PROC SORT DATA=ccs_diabetes  nodupkey; 
BY unique_hosp_count_id ccs_diabetes_hosp;
RUN;

/*ccs_fed*/
DATA  ccs_fed (compress=yes); 
SET  ccs_fed ;
ccs_fed_hosp=1;
keep unique_hosp_count_id singlelevel_ccs ccs_fed_hosp;
RUN;

PROC SORT DATA=ccs_fed  nodupkey; 
BY unique_hosp_count_id ccs_fed_hosp;
RUN;

/*ccs_gas*/
DATA  ccs_gas (compress=yes); 
SET  ccs_gas ;
ccs_gas_hosp=1;
keep unique_hosp_count_id singlelevel_ccs ccs_gas_hosp;
RUN;

PROC SORT DATA=ccs_gas  nodupkey; 
BY unique_hosp_count_id ccs_gas_hosp;
RUN;

/*ccs_osteoarthritis*/
DATA  ccs_osteoarthritis (compress=yes); 
SET  ccs_osteoarthritis ;
ccs_osteoarthritis_hosp=1;
keep unique_hosp_count_id singlelevel_ccs ccs_osteoarthritis_hosp;
RUN;

PROC SORT DATA=ccs_osteoarthritis  nodupkey; 
BY unique_hosp_count_id ccs_osteoarthritis_hosp;
RUN;

/*ccs_pneumonia*/
DATA  ccs_pneumonia (compress=yes); 
SET  ccs_pneumonia ;
ccs_pneumonia_hosp=1;
keep unique_hosp_count_id singlelevel_ccs ccs_pneumonia_hosp;
RUN;

PROC SORT DATA=ccs_pneumonia  nodupkey; 
BY unique_hosp_count_id ccs_pneumonia_hosp;
RUN;


/*ccs_renal*/
DATA  ccs_renal (compress=yes); 
SET  ccs_renal ;
ccs_renal_hosp=1;
keep unique_hosp_count_id singlelevel_ccs ccs_renal_hosp;
RUN;

PROC SORT DATA=ccs_renal  nodupkey; 
BY unique_hosp_count_id ccs_renal_hosp;
RUN;


/*ccs_respiratory*/
DATA  ccs_respiratory (compress=yes); 
SET  ccs_respiratory ;
ccs_respiratory_hosp=1;
keep unique_hosp_count_id singlelevel_ccs ccs_respiratory_hosp;
RUN;

PROC SORT DATA=ccs_respiratory  nodupkey; 
BY unique_hosp_count_id ccs_respiratory_hosp;
RUN;

/*ccs_septicemia*/
DATA  ccs_septicemia (compress=yes);
SET  ccs_septicemia ;
ccs_septicemia_hosp=1;
keep unique_hosp_count_id singlelevel_ccs ccs_septicemia_hosp;
RUN;

PROC SORT DATA=ccs_septicemia  nodupkey; 
BY unique_hosp_count_id ccs_septicemia_hosp;
RUN;

/*ccs_skin*/
DATA  ccs_skin (compress=yes); 
SET  ccs_skin ;
ccs_skin_hosp=1;
keep unique_hosp_count_id singlelevel_ccs ccs_skin_hosp;
RUN;

PROC SORT DATA=ccs_skin  nodupkey; 
BY unique_hosp_count_id ccs_skin_hosp;
RUN;

/*ccs_spondylosis*/
DATA  ccs_spondylosis (compress=yes); 
SET  ccs_spondylosis ;
ccs_spondylosis_hosp=1;
keep unique_hosp_count_id singlelevel_ccs ccs_spondylosis_hosp;
RUN;

PROC SORT DATA=ccs_spondylosis  nodupkey; 
BY unique_hosp_count_id ccs_spondylosis_hosp;
RUN;

/*ccs_uti*/
DATA  ccs_uti (compress=yes); 
SET  ccs_uti ;
ccs_uti_hosp=1;
keep unique_hosp_count_id singlelevel_ccs ccs_uti_hosp;
RUN;

PROC SORT DATA=ccs_uti  nodupkey; 
BY unique_hosp_count_id ccs_uti_hosp;
RUN;

/*merge the hosp-level variables to the hosp-level VAPD 2013-2017 dataset*/
PROC SORT DATA=VAPD_20132017_v2   OUT=VAPD_hosp2013217  (compress=yes); 
BY  unique_hosp_count_id datevalue;
RUN;

PROC SORT DATA=VAPD_hosp2013217 nodupkey;
BY  unique_hosp_count_id;
run;

/*get pulseox (SO) indicators*/
PROC SORT DATA=SO_hosp_only  nodupkey  OUT= hosp_v1; 
BY   unique_hosp_count_id;
RUN;

DATA hosp_v1 (compress=yes);
SET  hosp_v1;
PulseOx_hosp_ind =1;
RUN;

PROC SQL;
	CREATE TABLE  VAPD_hosp2013217_v1 (compress=yes)  AS 
	SELECT A.*, B.icu as icu_hosp, c.proccode_mechvent_hosp, d.diuretics_hosp, e.Sepsis_hosp , f.CHF_hosp , g.pulm_hosp , h.PulseOx_hosp_ind 
	FROM   VAPD_hosp2013217  A
	LEFT JOIN Icu_hosp  B ON A.unique_hosp_count_id =B.unique_hosp_count_id 
    left join Mechvent_hosp C on a.unique_hosp_count_id=c.unique_hosp_count_id
	left join Diuretics_hosp D on a.unique_hosp_count_id=d.unique_hosp_count_id
	left join Sepsis_hosp E on a.unique_hosp_count_id=e.unique_hosp_count_id
    left join  CHF_hosp f on a.unique_hosp_count_id=f.unique_hosp_count_id
	left join pulm_hosp g on a.unique_hosp_count_id=g.unique_hosp_count_id
	left join hosp_v1 h on a.unique_hosp_count_id=h.unique_hosp_count_id;
QUIT;

PROC SQL;
	CREATE TABLE  VAPD_hosp2013217_v2 (compress=yes)  AS 
	SELECT A.*, B.Ccs_alcohol_hosp, c.Ccs_ami_hosp, d.Ccs_atherosclerosis_hosp, e.Ccs_cardiac_hosp, f.Ccs_chestpain_hosp , g.Ccs_chf_hosp, h.Ccs_compdevice_hosp , i.Ccs_compsurge_hosp, j.Ccs_copd_hosp, k.Ccs_diabetes_hosp
	FROM  VAPD_hosp2013217_v1  A
	LEFT JOIN Ccs_alcohol B ON A.unique_hosp_count_id =B.unique_hosp_count_id 
    left join Ccs_ami C on a.unique_hosp_count_id=c.unique_hosp_count_id
	left join Ccs_atherosclerosis D on a.unique_hosp_count_id=d.unique_hosp_count_id
	left join Ccs_cardiac E on a.unique_hosp_count_id=e.unique_hosp_count_id
    left join  Ccs_chestpain f on a.unique_hosp_count_id=f.unique_hosp_count_id
	left join Ccs_chf g on a.unique_hosp_count_id=g.unique_hosp_count_id
	left join Ccs_compdevice h on a.unique_hosp_count_id=h.unique_hosp_count_id
	left join  Ccs_compsurge i on a.unique_hosp_count_id=i.unique_hosp_count_id
	left join Ccs_copd j on a.unique_hosp_count_id=j.unique_hosp_count_id
	left join Ccs_diabetes k on a.unique_hosp_count_id=k.unique_hosp_count_id;
QUIT;


PROC SQL;
	CREATE TABLE  VAPD_hosp2013217_v3 (compress=yes)  AS 
	SELECT A.*, B.Ccs_fed_hosp, c.Ccs_gas_hosp, d.Ccs_osteoarthritis_hosp, e.Ccs_pneumonia_hosp, f.Ccs_renal_hosp , g.Ccs_respiratory_hosp, h.Ccs_septicemia_hosp , i.Ccs_skin_hosp, j.Ccs_spondylosis_hosp, k.Ccs_uti_hosp
	FROM  VAPD_hosp2013217_v2  A
	LEFT JOIN Ccs_fed B ON A.unique_hosp_count_id =B.unique_hosp_count_id 
    left join Ccs_gas C on a.unique_hosp_count_id=c.unique_hosp_count_id
	left join Ccs_osteoarthritis D on a.unique_hosp_count_id=d.unique_hosp_count_id
	left join Ccs_pneumonia E on a.unique_hosp_count_id=e.unique_hosp_count_id
    left join  Ccs_renal f on a.unique_hosp_count_id=f.unique_hosp_count_id
	left join Ccs_respiratory g on a.unique_hosp_count_id=g.unique_hosp_count_id
	left join Ccs_septicemia h on a.unique_hosp_count_id=h.unique_hosp_count_id
	left join  Ccs_skin i on a.unique_hosp_count_id=i.unique_hosp_count_id
	left join Ccs_spondylosis j on a.unique_hosp_count_id=j.unique_hosp_count_id
	left join Ccs_uti k on a.unique_hosp_count_id=k.unique_hosp_count_id;
QUIT;

DATA VAPD_hosp2013217_v3b  (compress=yes);
SET  VAPD_hosp2013217_v3;
if Ccs_alcohol_hosp  NE 1 then Ccs_alcohol_hosp =0; if Ccs_ami_hosp NE 1 then Ccs_ami_hosp =0;
if Ccs_atherosclerosis_hosp NE 1 then  Ccs_atherosclerosis_hosp=0;if Ccs_cardiac_hosp NE 1 then Ccs_cardiac_hosp =0;
if Ccs_chestpain_hosp  NE 1 then Ccs_chestpain_hosp  =0;if Ccs_chf_hosp NE 1 then Ccs_chf_hosp =0;
if Ccs_compdevice_hosp NE 1 then Ccs_compdevice_hosp =0;if Ccs_compsurge_hosp NE 1 then Ccs_compsurge_hosp =0;
if Ccs_copd_hosp NE 1 then Ccs_copd_hosp =0;if Ccs_diabetes_hosp NE 1 then Ccs_diabetes_hosp =0;
if Ccs_fed_hosp NE 1 then  Ccs_fed_hosp=0;if Ccs_gas_hosp NE 1 then Ccs_gas_hosp =0;
if Ccs_osteoarthritis_hosp NE 1 then Ccs_osteoarthritis_hosp =0;if Ccs_pneumonia_hosp  NE 1 then Ccs_pneumonia_hosp =0;
if Ccs_renal_hosp NE 1 then Ccs_renal_hosp =0;if Ccs_respiratory_hosp NE 1 then Ccs_respiratory_hosp =0;
if Ccs_septicemia_hosp NE 1 then  Ccs_septicemia_hosp=0;if Ccs_skin_hosp NE 1 then Ccs_skin_hosp =0;
if Ccs_spondylosis_hosp NE 1 then Ccs_spondylosis_hosp =0;if Ccs_uti_hosp  NE 1 then  Ccs_uti_hosp=0;
if icu_hosp NE 1 then icu_hosp =0;if proccode_mechvent_hosp NE 1 then proccode_mechvent_hosp =0;
if diuretics_hosp NE 1 then  diuretics_hosp=0;if Sepsis_hosp NE 1 then Sepsis_hosp =0;
if CHF_hosp  NE 1 then  CHF_hosp =0;if pulm_hosp NE 1 then pulm_hosp =0; 
if PulseOx_hosp_ind NE 1 then PulseOx_hosp_ind=0;
RUN;

PROC SORT DATA=VAPD_hosp2013217_v3b;
BY  PulseOx_hosp_ind;
RUN;

PROC FREQ DATA=VAPD_hosp2013217_v3b;  
TABLE  PulseOx_hosp_ind;
RUN;

PROC MEANS DATA=VAPD_hosp2013217_v3b   MEDIAN Q1 Q3;
class PulseOx_hosp_ind;
VAR age hosp_los ;
RUN;

PROC FREQ DATA=VAPD_hosp2013217_v3b; 
by PulseOx_hosp_ind;
TABLE female race CHF_hosp  pulm_hosp;
RUN;

PROC FREQ DATA=VAPD_hosp2013217_v3b; 
by PulseOx_hosp_ind;
TABLE Ccs_alcohol_hosp Ccs_ami_hosp  Ccs_atherosclerosis_hosp Ccs_cardiac_hosp Ccs_chestpain_hosp Ccs_osteoarthritis_hosp Ccs_renal_hosp  Ccs_septicemia_hosp Ccs_skin_hosp Ccs_uti_hosp 
Ccs_chf_hosp  Ccs_compdevice_hosp  Ccs_compsurge_hosp Ccs_copd_hosp Ccs_diabetes_hosp  Ccs_fed_hosp   Ccs_gas_hosp  Ccs_pneumonia_hosp Ccs_respiratory_hosp Ccs_spondylosis_hosp;
RUN;

PROC FREQ DATA=VAPD_hosp2013217_v3b; 
by PulseOx_hosp_ind;
TABLE region new_teaching inhosp_mort mort30_admit icu_hosp proccode_mechvent_hosp diuretics_hosp Sepsis_hosp;
RUN;

/*revision also get p-values*/
proc ttest;
class PulseOx_hosp_ind;
var  age ;
run;

proc ttest;
class PulseOx_hosp_ind;
var  hosp_los;
run;


PROC FREQ DATA=VAPD_hosp2013217_v3b; 
TABLEs female*PulseOx_hosp_ind race*PulseOx_hosp_ind / chisq measures;
RUN;

PROC FREQ DATA=VAPD_hosp2013217_v3b; 
TABLEs CHF_hosp*PulseOx_hosp_ind pulm_hosp*PulseOx_hosp_ind / chisq measures;
RUN;

PROC FREQ DATA=VAPD_hosp2013217_v3b; 
TABLE region*PulseOx_hosp_ind
new_teaching*PulseOx_hosp_ind
inhosp_mort*PulseOx_hosp_ind
mort30_admit*PulseOx_hosp_ind
icu_hosp*PulseOx_hosp_ind
proccode_mechvent_hosp*PulseOx_hosp_ind
diuretics_hosp*PulseOx_hosp_ind
Sepsis_hosp*PulseOx_hosp_ind / chisq measures;
RUN;


PROC FREQ DATA=VAPD_hosp2013217_v3b; 
TABLE Ccs_alcohol_hosp*PulseOx_hosp_ind
Ccs_ami_hosp*PulseOx_hosp_ind
Ccs_atherosclerosis_hosp*PulseOx_hosp_ind
Ccs_cardiac_hosp*PulseOx_hosp_ind
Ccs_chestpain_hosp*PulseOx_hosp_ind
Ccs_osteoarthritis_hosp*PulseOx_hosp_ind
Ccs_renal_hosp*PulseOx_hosp_ind
Ccs_septicemia_hosp*PulseOx_hosp_ind
Ccs_skin_hosp*PulseOx_hosp_ind
Ccs_uti_hosp *PulseOx_hosp_ind
Ccs_chf_hosp*PulseOx_hosp_ind
Ccs_compdevice_hosp *PulseOx_hosp_ind
Ccs_compsurge_hosp*PulseOx_hosp_ind
Ccs_copd_hosp*PulseOx_hosp_ind
Ccs_diabetes_hosp*PulseOx_hosp_ind
Ccs_fed_hosp *PulseOx_hosp_ind
Ccs_gas_hosp*PulseOx_hosp_ind
Ccs_pneumonia_hosp *PulseOx_hosp_ind
Ccs_respiratory_hosp*PulseOx_hosp_ind
Ccs_spondylosis_hosp*PulseOx_hosp_ind / chisq measures;
RUN;
