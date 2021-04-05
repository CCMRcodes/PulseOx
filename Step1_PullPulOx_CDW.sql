/*Step 1, pull data from CDW*/
/*This SQL code below pulls Pulse Oximetry values from the Vitals Domain in CDW*/
/*Authour: Xiao Qing (Shirley) Wang*/
/*Date: 3/31/2021*/

use <STUDY NAME>
go

SELECT distinct a.Sta3n, a.PatientSID, a.vitalSignTakenDateTime, a.VitalResultNumeric, a.SupplementalO2, a.VitalTypeSID,
        B.VitalType, c.patienticn
into dflt.PulseOx 
FROM [Src].[Vital_VitalSign] as   A
left JOIN [CDWWORK].[Dim].[VitalType] as  B ON A.VitalTypeSID =B.VitalTypeSID
left join  Src.SPatient_SPatient c on a.patientsid=c.patientsid
WHERE (a.VitalSignTakenDateTime >= 'YYYYMMDD' and a.VitalSignTakenDateTime < 'YYYYMMDD') /*times pulled*/
AND (
/*1) pull all where SupplementalO2 not null, type=PulseOX and VitalResultNumeric > 0 and VitalResultNumeric <= 100 */
(a.SupplementalO2 <> 'NULL' and  b.VitalType='PULSE OXIMETRY' and a.VitalResultNumeric > 0 and a.VitalResultNumeric <= 100)

 /*OR 2) also pull pulseox, and SupplementalO2 is NULL and VitalResultNumeric > 0 and VitalResultNumeric <= 100*/
OR (a.SupplementalO2 is NULL and b.VitalType='PULSE OXIMETRY' and a.VitalResultNumeric > 0 and a.VitalResultNumeric <= 100)
) 


/*check distinct vitaltype*/
select distinct vitaltype from dflt.PulseOx  --only  PULSE OXIMETRY 


 /*compress table*/
alter table dflt.PulseOx
rebuild partition=ALL
with
(data_compression=page)

//*Download dflt.PulseOx into SAS Table for Step 2 Cleaning*/