/*************************************************************************/
/* Employed and Unemployed by NAICS and Region                           */
/*************************************************************************/

DATA work.lfstats4digNAICS;
SET RTRAdata.LFS201115 (keep = ID PROV AGE ERTAB LFSSTAT NAICS_5 SYEAR SMTH);

/* Province B.C.*/
if PROV = 59;

/* Age 15 or over */
if AGE >= 15;

/*Labour Force Status*/

length lf_stat $20;

if LFSSTAT IN (1,2) then lf_stat = 'Employed';
else if LFSSTAT in (3,4,5) then lf_stat = 'Unemployed';
else lf_stat = 'Unknown';

/*Economic Region - British Columbia*/
length BC_Region $ 30;
IF   ERTAB IN ("5910") THEN BC_Region="Vancouver Island and Coast";
ELSE IF ERTAB IN ("5920") THEN BC_Region="Lower Mainland-Southwest";
ELSE IF ERTAB IN ("5930") THEN BC_Region="Thompson-Okanagan";
ELSE IF ERTAB IN ("5940") THEN BC_Region="Kootenay";
ELSE IF ERTAB IN ("5950") THEN BC_Region="Cariboo";
ELSE IF ERTAB IN ("5960") THEN BC_Region="North Coast";
ELSE IF ERTAB IN ("5970") THEN BC_Region="Nechako";
ELSE IF ERTAB IN ("5980") THEN BC_Region="Northeast";
ELSE BC_Region="Other";

/*only non-missing naics*/
if NAICS_5 = ' ' then delete;

run;

%RTRAFreq(
     InputDataset=work.lfstats4digNAICS,
     OutputName=lfsstat4digNAICS,
     ClassVarList= SYEAR SMTH BC_Region lf_stat NAICS_5,
     UserWeight=FINALWT);
run;
