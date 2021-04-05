/*Step 2: Clean Pulse Oximetry (PulseOx) values pulled from CDW for study years of interest*/
/*Author: Xiao Qing (Shirley) Wang*/
/*date: 3/31/21*/


libname vital 'PulseOx Datasets';


/********************************************************************************************************************/
/*clean PulseOx data*/
DATA NEWPULSEOX20132017 (compress=yes); /*49008337*/
SET vital.pulseox; /*This is the dataset downloaded into SAS table from CDW in Step 1*/
RUN;

/*remove any duplicates*/
PROC SORT DATA=NEWPULSEOX20132017  nodupkey; /*49008337, 0 dups*/
BY  PatientSID sta3n vitalSignTakenDateTime VitalResultNumeric SupplementalO2;
RUN;

PROC MEANS DATA=NEWPULSEOX20132017  MIN MAX MEAN MEDIAN Q1 Q3;
VAR  VitalResultNumeric;
RUN;

/*check VitalType*/
PROC FREQ DATA=NEWPULSEOX20132017  order=freq;
TABLE  VitalType;
RUN;

DATA  all_data (compress=yes); 
SET NEWPULSEOX20132017;
if SupplementalO2 ='' then LPM=0;
RUN;

PROC FREQ DATA=all_data  order=freq; /*36,752,477 records have LPM=0, 12,255,860 records where SupplementalO2  <> NULL*/
TABLE  LPM;
RUN;


/*Clean those 17,469,574 records where SupplementalO2  <> NULL:*/
DATA need_clean (compress=yes)  
     dont_clean (compress=yes);
SET  all_data;
vital_date=datepart(VitalSignTakenDateTime);
format vital_date mmddyy10.;
year=year(vital_date);
if LPM NE 0 then output need_clean;
else output dont_clean;
RUN;

/*set up the dont_clean data*/
DATA dont_clean_cohort (compress=yes); 
retain patienticn PatientSID Sta3n VitalTypeSID VitalType  vitalSignTakenDateTime vital_date year  
VitalResultNumeric SupplementalO2 SpO2  O2_LPM  incoherent;
SET dont_clean;
SpO2=VitalResultNumeric;
incoherent=0;
O2_LPM=LPM;
keep patienticn PatientSID Sta3n VitalTypeSID VitalType  vitalSignTakenDateTime vital_date year  
VitalResultNumeric SupplementalO2 SpO2  O2_LPM  incoherent; 
RUN;

PROC MEANS DATA=dont_clean_cohort   MIN MAX MEAN MEDIAN Q1 Q3;
VAR VitalResultNumeric;
RUN;

proc sgplot data=dont_clean_cohort noautolegend;
 histogram VitalResultNumeric;
 density VitalResultNumeric;
run;

PROC FREQ DATA=dont_clean_cohort; /*Increasing trend*/
TABLE  year;
RUN;

PROC FREQ DATA=need_clean; /*Increasing trend*/
TABLE  year;
RUN;

DATA PulseOx20132017_v1 (compress=yes); 
SET need_clean;
obs=_N_;
drop LPM;
RUN;

/*Clean the PulseOX data, no need to merge to VAPD dataset yet*/
/*how many obs have VitalResultNumeric > 100?*/
DATA check_value (compress=yes); /*N=0*/
SET PulseOx20132017_v1;
if  VitalResultNumeric>100;
RUN;

PROC MEANS DATA=PulseOx20132017_v1   MIN MAX MEAN MEDIAN Q1 Q3;
VAR  VitalResultNumeric;
RUN;

/*clean the SupplementalO2 to take out extra space and turn everything into CAPS*/
DATA  PulseOx20132017_v2  (compress=yes rename=SupplementalO2_v3=SupplementalO2); 
SET  PulseOx20132017_v1;
SupplementalO2_v2=upcase(SupplementalO2); /*turn all units into uppercase*/
SupplementalO2_v3=compress(SupplementalO2_v2);  /*removes all blanks*/
drop SupplementalO2_v2 SupplementalO2;  /*drop the original SupplementalO2 field and rename SupplementalO2_v3 as SupplementalO2*/
RUN;

/******************************************************************************************************/
PROC SQL;
CREATE TABLE pulse_noLmin (compress=yes)  AS  
SELECT *
FROM PulseOx20132017_v2
WHERE   SupplementalO2 not like  '%L/MIN%';
QUIT;

PROC MEANS DATA=pulse_noLmin   MIN MAX MEAN MEDIAN Q1 Q3;
VAR  VitalResultNumeric;
RUN;

PROC FREQ DATA=pulse_noLmin  order=freq;
TABLE  SupplementalO2;
RUN;

/*remove the % sign and change to numeric value and look at descriptive*/
DATA  pulse_noLmin2  (compress=yes); 
SET   pulse_noLmin;
supple_char=compress(SupplementalO2,'%'); 
supple_num=input(supple_char, 3.);
RUN;

/* check if supple_num NE VitalResultNumeric*/
data checking; 
set pulse_noLmin2 ;
if supple_num NE VitalResultNumeric;
run;

PROC FREQ DATA=pulse_noLmin2;
TABLE  supple_num;
RUN;

PROC MEANS DATA=pulse_noLmin2   MIN /*2*/ MAX/*100*/ MEAN /*64*/ MEDIAN /*60*/ Q1 /*35*/ Q3 /*96*/;
VAR  supple_num;
RUN;

/*use Jack's conversions to get SpO2 and O2 (L/MIN), call this cohort1_20132017.
create new fields: SpO2=VitalResultNumeric, O2_LPM, and incoherent*/
DATA cohort1_20132017_test   (compress=yes);
SET  pulse_noLmin2;
SpO2=VitalResultNumeric;
/*incoherent=0;*/
O2_LPM=((supple_num/100)-0.21)/0.03; /*turn into % first*/
if O2_LPM < 0 then delete; /*on 3/2/20 Jack said it's okay to delete*/
RUN;

/*check*/
/*What to do with N=24 where O2_LPM is negative, due to small % SupplementalO2 values. Jack said to delete*/
/*data negative; /*24*/*/
/*set  cohort1_20132017_test;*/
/*where O2_LPM < 0;*/
/*keep  vital_date VitalResultNumeric SupplementalO2  SpO2  O2_LPM;*/
/*run;*/;

PROC MEANS DATA=cohort1_20132017_test   MIN MAX MEAN MEDIAN Q1 Q3;
VAR  VitalResultNumeric SpO2  O2_LPM;
RUN;

PROC FREQ DATA=cohort1_20132017_test  order=freq;
TABLE   O2_LPM;
RUN;

/*there's cohor 1A and 1B: in general, SupplementalO2 should not equal VitalResultNumeric, Jack said this is incoherent on 6/17/20.*/
DATA cohort1A_20132017 (compress=yes) 
     cohort1B_20132017 (compress=yes);
SET  cohort1_20132017_test;
if supple_num=  VitalResultNumeric then  incoherent=1;
 else incoherent=0;
if incoherent=0 then output cohort1A_20132017;
if incoherent=1 then output cohort1B_20132017;
keep patienticn PatientSID Sta3n VitalTypeSID VitalType obs vitalSignTakenDateTime vital_date year  
VitalResultNumeric SupplementalO2 SpO2  O2_LPM  incoherent;
RUN;

DATA cohort1A_20132017 (compress=yes);
retain patienticn PatientSID Sta3n VitalTypeSID VitalType obs vitalSignTakenDateTime vital_date year  
VitalResultNumeric SupplementalO2 SpO2  O2_LPM  incoherent;
set cohort1A_20132017;
RUN;

DATA cohort1B_20132017 (compress=yes); 
retain patienticn PatientSID Sta3n VitalTypeSID VitalType obs vitalSignTakenDateTime vital_date year  
VitalResultNumeric SupplementalO2 SpO2  O2_LPM  incoherent;
set cohort1B_20132017;
RUN;

/******************************************************************************************************************/
/*select everything else (none % only) not in pulse_noLmin dataset, and do a frequency to see what units it entail*/
PROC SQL;
CREATE TABLE  pulse_lmin  (COMPRESS=YES) AS 
SELECT A.* FROM PulseOx20132017_v2 AS A
WHERE A.obs not IN (SELECT obs FROM pulse_noLmin);
QUIT;

PROC FREQ DATA=pulse_lmin  order=freq;
TABLE SupplementalO2;
RUN;

/*******************/
/*look at those only with L/MIN units*/
PROC SQL;
CREATE TABLE pulse_lmin_only (compress=yes)  AS  
SELECT *
FROM pulse_lmin
WHERE   SupplementalO2  like  '%L/MIN' or SupplementalO2  like  'L/MIN';
QUIT;

/*check*/
PROC FREQ DATA=pulse_lmin_only  order=freq;
TABLE  SupplementalO2;
RUN;

/*there were some typs for LPM, remove 'L/MIN', turn into numeric to look at descriptives*/
DATA pulse_lmin_only2 (compress=yes); 
SET  pulse_lmin_only ;
if SupplementalO2='3EL/MIN' then SupplementalO2='3L/MIN';
if SupplementalO2='5LTL/MIN' then SupplementalO2='5L/MIN';
if SupplementalO2='4LITERSL/MIN' then SupplementalO2='4L/MIN';
if SupplementalO2='3LPML/MIN' then SupplementalO2='3L/MIN';
if SupplementalO2='4LPML/MIN' then SupplementalO2='4L/MIN';
if SupplementalO2='2L.NCL/MIN' then SupplementalO2='2L/MIN';
if SupplementalO2='2LPML/MIN' then SupplementalO2='2L/MIN';
if SupplementalO2='3LTL/MIN' then SupplementalO2='3L/MIN';
if SupplementalO2='RAL/MIN' then SupplementalO2='0L/MIN';
SupplementalO2_char=compress(SupplementalO2,'L/MIN'); /*removes '.' in units*/
SupplementalO2_num=input(SupplementalO2_char, 3.);
RUN;

PROC FREQ DATA=pulse_lmin_only2  order=freq;
TABLE  SupplementalO2;
RUN;

PROC MEANS DATA=pulse_lmin_only2   MIN  MAX MEAN  MEDIAN  Q1 Q3;
VAR SupplementalO2_num VitalResultNumeric;
RUN;

/*use Jack's conversions to get Saturation (L/MIN), call this  cohort2.*/
DATA cohort2_20132017_test  (compress=yes);
SET pulse_lmin_only2;
SpO2=VitalResultNumeric;
incoherent=0;
O2_LPM=SupplementalO2_num;
if SupplementalO2='RAL/MIN' then O2_LPM=0; /*recode RAL/MIN =0, don't exclude*/
if O2_LPM =. then delete;  /*3/2/20: Jack said it's ok to delete. N=14*/
RUN;

/*check*/
PROC MEANS DATA=cohort2_20132017_test  MIN MAX MEAN MEDIAN Q1 Q3;
VAR  VitalResultNumeric O2_LPM SpO2;
RUN;
PROC FREQ DATA=cohort2_20132017_test  order=freq;
TABLE  incoherent O2_LPM;
RUN;

/*Question, what to do with these?*/
/*data missings_LPM; /*14*/*/
/*set cohort2_20132017_test;*/
/*if O2_LPM =.;*/
/*keep  vital_date VitalResultNumeric SupplementalO2  SpO2  O2_LPM;*/
/*run;*/;

DATA cohort2_20132017 (compress=yes);  
retain patienticn PatientSID Sta3n VitalTypeSID VitalType obs vitalSignTakenDateTime vital_date year  
VitalResultNumeric SupplementalO2 SpO2  O2_LPM  incoherent;
SET   cohort2_20132017_test;
drop SupplementalO2_char SupplementalO2_num;
RUN;



/*********************************************************************************************************************/
/*look at those only with 'L/MIN%' unit*/
PROC SQL;
CREATE TABLE pulse_lminpercent_only (compress=yes)  AS  
SELECT *
FROM pulse_lmin
WHERE   SupplementalO2  like  'L/MIN%';
QUIT;

PROC FREQ DATA= pulse_lminpercent_only  order=freq;
TABLE  SupplementalO2;
RUN;

/* get the descriptive on X%*/
/*first, get the last 4 digits*/
DATA last_4char_pulse_lminpercent  (compress=yes); 
SET  pulse_lminpercent_only ;
last_4=substr(SupplementalO2,length(SupplementalO2)-3,4);
RUN;

PROC FREQ DATA=last_4char_pulse_lminpercent  order=freq;
TABLE  last_4;
RUN;

/*from last character values, compress "MIN%" characters*/
DATA last_4char_pulse_lminpercent2  (compress=yes); 
SET  last_4char_pulse_lminpercent  ;
last_4char_v2=compress(last_4,'M');
last_4char_v3=compress(last_4char_v2,'I');
last_4char_v4=compress(last_4char_v3,'N');
last_4char_v5=compress(last_4char_v4,'%');
last_4num=input(last_4char_v5,3.);
RUN;

PROC FREQ DATA= last_4char_pulse_lminpercent2  order=freq;
TABLE  last_4num last_4char_v5;
RUN;

DATA cohort3232 (compress=yes); 
SET last_4char_pulse_lminpercent2;
if last_4num NE .;
if last_4num = VitalResultNumeric then equal=1; else equal=0;
RUN;

PROC FREQ DATA=cohort3232  order=freq;
TABLE equal; /*18905=equal*/
RUN;

PROC MEANS DATA=cohort3232  MIN MAX MEAN MEDIAN Q1 Q3;
VAR  last_4num VitalResultNumeric;
RUN;

/*use Jack's conversions to get Saturation (L/MIN), call this cohort3A*/
DATA cohort3a_20132017_test (compress=yes); 
SET last_4char_pulse_lminpercent2;
if SupplementalO2='L/MIN%';
SpO2=VitalResultNumeric;
O2_LPM=0;
incoherent=0;
RUN;

/*check Saturation_LPM cohorts*/
PROC MEANS DATA=cohort3a_20132017_test    MIN MAX MEAN MEDIAN Q1 Q3;
VAR  SpO2 O2_LPM;
RUN;
PROC FREQ DATA=cohort3a_20132017_test  order=freq;
TABLE O2_LPM SupplementalO2;
RUN;

DATA cohort3a_20132017 (compress=yes); 
retain patienticn PatientSID Sta3n VitalTypeSID VitalType obs vitalSignTakenDateTime vital_date year  
VitalResultNumeric SupplementalO2 SpO2  O2_LPM  incoherent;
SET cohort3a_20132017_test;
drop last_4 last_4char_v2-last_4char_v5 last_4num;
RUN;

/*Jack decided to label these as incoherent*/
DATA cohort3b_20132017_test   (compress=yes); 
SET last_4char_pulse_lminpercent2;
if SupplementalO2 NE 'L/MIN%';
SpO2=VitalResultNumeric;
O2_LPM=.; /*turn into % first*/
incoherent=1;
run;

/*Jack said he wants to look at distribution of the nn% */
DATA  cohort3b_20132017  (compress=yes); 
retain patienticn PatientSID Sta3n VitalTypeSID VitalType obs vitalSignTakenDateTime vital_date year  
VitalResultNumeric SupplementalO2 SpO2  O2_LPM  incoherent;
SET  cohort3b_20132017_test ;
/*drop last_4 last_4char_v2-last_4char_v5 last_4num;*/
RUN;

PROC MEANS DATA=cohort3b_20132017   MIN MAX MEAN MEDIAN Q1 Q3;
VAR last_4num ;
RUN;

proc sgplot data=cohort3b_20132017 noautolegend;
 histogram last_4num;
 density last_4num;
run;



/*******************************************************************************************************************/
/*combine pulse_lminpercent_only+pulse_lmin_only+pulse_noLmin2 and see what is not in those datasets, do frequency check of those units*/
DATA all (compress=yes); 
SET pulse_lminpercent_only pulse_lmin_only pulse_noLmin2;
RUN;

PROC SQL;
CREATE TABLE  whatisleft  (COMPRESS=YES) AS 
SELECT A.* FROM PulseOx20132017_v2 AS A
WHERE A.obs not IN (SELECT obs FROM work.all);
QUIT;

PROC FREQ DATA=whatisleft  order=freq;
TABLE  SupplementalO2;
RUN;


/*based on Jack's notes: if XX% not equal vitalresultsnumeric, then pull their TIU notes for validation, for "whatisleft" cohort*/
/*1) separate out the ##%*/ /*first, get the last 4 digits*/
DATA last_4charV1 (compress=yes); 
SET whatisleft;
last_4=substr(SupplementalO2,length(SupplementalO2)-3,4);
RUN;

/*look at list*/
PROC FREQ DATA=last_4charV1   order=freq;
TABLE  last_4;
RUN;

/*2/3/20: look at descriptive for this 126,091 cohort with only nnL/MIN%*/
DATA LMINpercentonly_v1 (compress=yes); 
SET last_4charV1 ;
if last_4 = 'MIN%';
RUN;

PROC MEANS DATA=LMINpercentonly_v1   MIN MAX MEAN MEDIAN Q1 Q3;
VAR  VitalResultNumeric;
RUN;

/*get the first 4 characters*/
DATA LMINpercentonly_v2  (compress=yes);
SET  LMINpercentonly_v1 ;
first_4=substr(SupplementalO2,1,4);
RUN;

PROC FREQ DATA=LMINpercentonly_v2  order=freq;
TABLE  first_4;
RUN;

/*compress L/M*/
DATA  LMINpercentonly_v3;
SET  LMINpercentonly_v2;
first_4char_v2=compress(first_4,'M');
first_4char_v3=compress(first_4char_v2,'/');
first_4char_v4=compress(first_4char_v3,'L');
first_4num=input(first_4char_v4,3.);
RUN;

PROC MEANS DATA= LMINpercentonly_v3  MIN MAX MEAN MEDIAN Q1 Q3;
VAR  first_4num VitalResultNumeric;
RUN;


/************/
/*Question: some last 4 characters have PRN%, NLT%, TO2%, exclude them?*/
PROC FREQ DATA=last_4charV1  order=freq;
TABLE  last_4;
RUN;


/*compress 1 step at a time*/
DATA  last_4charV2;  
SET  last_4charV1;
/*if Jack is ok with excluding the weird last 4 characters. Jack ok with deleting these on 3/2/20*/
if last_4 in ('NNC%','INO%','NLT%','PRN%','TO2%') then delete;
last_4char_v2=compress(last_4,'M');
last_4char_v3=compress(last_4char_v2,'I');
last_4char_v4=compress(last_4char_v3,'N');
last_4char_v5=compress(last_4char_v4,'%');
last_4num=input(last_4char_v5,3.);
RUN;

PROC FREQ DATA=last_4charV2   order=freq; 
TABLE  last_4char_v5;
RUN;

PROC MEANS DATA=last_4charV2   MIN MAX MEAN MEDIAN Q1 Q3; /*median=95.00, mean=83*/
VAR  last_4num;
RUN;

PROC FREQ DATA=last_4charV2 ;
TABLE last_4num ;
RUN;

/*to see if there are any last_4num = VitalResultNumeric*/
DATA cohort_20132017_test ; 
SET last_4charV2;
if (last_4num NE . ) and (last_4num = VitalResultNumeric);
RUN;

/*2/3/20: look at nn L/Min descriptive*/
DATA cohort_20132017_test2  (compress=yes); 
SET cohort_20132017_test;
first_4=substr(SupplementalO2,1,4);
RUN;

PROC FREQ DATA=cohort_20132017_test2 order=freq;
TABLE  first_4;
RUN;

DATA cohort_20132017_test3 (compress=yes); 
SET cohort_20132017_test2;
first_4char_v2=compress(first_4,'M');
first_4char_v3=compress(first_4char_v2,'/');
first_4char_v4=compress(first_4char_v3,'L');
first_4num=input(first_4char_v4,3.);
RUN;

PROC MEANS DATA=cohort_20132017_test3   MIN MAX MEAN MEDIAN Q1 Q3;
VAR  first_4num VitalResultNumeric;
RUN;

/*use Jack's conversions to get Saturation (L/MIN), call this  cohort5.*/
DATA  cohort5_20132017_test  (compress=yes); 
SET  cohort_20132017_test3;
SpO2=VitalResultNumeric;
incoherent=0;
O2_LPM=first_4num;
RUN;

/*check*/
PROC MEANS DATA=cohort5_20132017_test   MIN MAX MEAN MEDIAN Q1 Q3;
VAR O2_LPM SpO2;
RUN;

DATA  cohort5_20132017  (compress=yes); 
retain patienticn PatientSID Sta3n VitalTypeSID VitalType obs vitalSignTakenDateTime vital_date year  
VitalResultNumeric SupplementalO2 SpO2  O2_LPM  incoherent;
SET  cohort5_20132017_test;
drop last_4  last_4char_v2-last_4char_v5  last_4num first_4  first_4char_v2-first_4char_v4 first_4num;
RUN;

/*use Jack's conversions to get Saturation (L/MIN), call this  cohort4.*/
DATA cohort4_20132017_test (compress=yes); 
SET LMINpercentonly_v3;
SpO2=VitalResultNumeric;
incoherent=0;
O2_LPM=first_4num;
RUN;

/*check*/
PROC MEANS DATA=cohort4_20132017_test   MIN MAX MEAN MEDIAN Q1 Q3;
VAR  SpO2 O2_LPM;
RUN;
PROC FREQ DATA=cohort4_20132017_test  order=freq;
TABLE  incoherent SpO2;
RUN;

DATA cohort4_20132017 (compress=yes); 
retain patienticn PatientSID Sta3n VitalTypeSID VitalType obs vitalSignTakenDateTime vital_date year  
VitalResultNumeric SupplementalO2 SpO2  O2_LPM  incoherent;
SET  cohort4_20132017_test ;
drop last_4  first_4  first_4num  first_4char_v2-first_4char_v4;
RUN;

/****************************************************************************************/
/*look at those that  last_4num not equal to vitalresultnumeric*/
DATA  val_cohort (compress=yes); 
SET last_4charV2;
if (last_4num NE . ) and (last_4num NE VitalResultNumeric);
RUN;


/*use Jack's conversions to get Saturation (L/MIN), call this  cohort6.*/
DATA cohort6_20132017_test (compress=yes); 
SET val_cohort;
SpO2=VitalResultNumeric;
incoherent=1;
O2_LPM=.;
RUN;

/*check*/
PROC MEANS DATA=cohort6_20132017_test   MIN MAX MEAN MEDIAN Q1 Q3;
VAR  O2_LPM SpO2;
RUN;
PROC FREQ DATA=cohort6_20132017_test  order=freq;
TABLE  incoherent ;
RUN;

DATA cohort6_20132017 (compress=yes); 
retain patienticn PatientSID Sta3n VitalTypeSID VitalType obs vitalSignTakenDateTime vital_date year  
VitalResultNumeric SupplementalO2 SpO2  O2_LPM  incoherent;
SET cohort6_20132017_test;
drop last_4  last_4char_v2-last_4char_v5 last_4num;
RUN;

/*combine all cohorts (cleaned and don't clean) and make sure the totals add up, drop the inconsistant cohorts*/
DATA vital.PulseOx_YYYYMMDD (compress=yes); 
SET DONT_CLEAN_COHORT COHORT1A_20132017  COHORT2_20132017 COHORT3A_20132017  COHORT4_20132017 COHORT5_20132017;
RUN;

