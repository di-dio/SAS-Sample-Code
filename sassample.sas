

****************************************;
* create listing of top prescribers     ;
****************************************;
%macro rankphys(mkt,levelletter,terrdist);

%mscan(&&&mkt.prd.,%nrquote(
%mscan(trx nrx, %nrquote(

	data &program.&levelletter._!_? (keep = &terrdist. id zsprod &mkt.mkt_!_C5wk
		&mkt.mkt_!_p5wk ?_!_C5wk ?_!_p5wk );
	   set curr_wk.w3physls_xpo_&mkt.&terrdist. (where = (pdrp~=1 ));
			if zsprod ="?" or zsprod = "&mkt." or zsprod = upcase("&mkt"); 
			?_!_C5wk = 0;
			?_!_P5wk = 0;
			&mkt.mkt_!_C5wk = 0;
			&mkt.mkt_!_P5wk = 0;

			if zsprod = "?" then do;
				?_!_C5wk = !_C5wk;
				?_!_P5wk = !_P5wk;
			end;
			if zsprod = "&mkt." or zsprod = upcase("&mkt.") then do;
				&mkt.mkt_!_C5wk = !_C5wk;
				&mkt.mkt_!_P5wk = !_P5wk;
				flag=1;
			end;
	run;
	
	%msortsum(&program.&levelletter._!_?,id &terrdist.,_NUMERIC_,&program.&levelletter._!_?,N);

	* create market shares *;
	data &program.&levelletter._!_?;
	  set &program.&levelletter._!_?; 
	  ?_!shr_c5wk = 0;
 	  ?_!shr_p5wk =0;
			if &mkt.mkt_!_c5wk>0 then do;
						?_!shr_c5wk = 
						?_!_c5wk/&mkt.mkt_!_c5wk;
			end;
			if &mkt.mkt_!_p5wk>0 then do;
						?_!shr_p5wk = 
						?_!_p5wk/&mkt.mkt_!_p5wk;
			end;
			* calculate change between periods *;
			 ?_!chg_5wk = ?_!_c5wk - ?_!_p5wk;
			 ?_!shrchg_5wk = ?_!shr_c5wk - ?_!shr_p5wk;
	run;


%mscan(_c5wk chg_5wk shrchg_5wk shr_c5wk,%nrquote(

		%msort(&program.&levelletter._!_?,id); * all ties broken by id *;
		%msort(&program.&levelletter._!_?,&terrdist. descending ?_!@);
		run;

		data &program.&levelletter._!_?;
		  set &program.&levelletter._!_?;
		    by &terrdist.;
			  if first.&terrdist. then rank_?_!@=0;
		      if &terrdist.^='' then rank_?_!@= rank_?_!@+1;
		      retain rank_?_!@;
	   	 run;

		 data &program.&levelletter._!_?;
		    set &program.&levelletter._!_?;
			if ?_!@<=0 then rank_?_!@=0;
		 run;
					                  ),%str(@));

	* finally, do a special ranking for "new to market";
	data &program.&levelletter._!_?none &program.&levelletter._!_?some;
		set &program.&levelletter._!_?;
		if ?_!_p5wk=0 and ?_!_c5wk>0 then output &program.&levelletter._!_?none;
		else output &program.&levelletter._!_?some;
	run;

		%msort(&program.&levelletter._!_?none,id); * all ties broken by id *;
		%msort(&program.&levelletter._!_?none,&terrdist. descending ?_!_c5wk);
		run;

		data &program.&levelletter._!_?none;
		  set &program.&levelletter._!_?none;
		    by &terrdist.;
			  if first.&terrdist. then rank_?_!_new=0;
		      if &terrdist.^='' then rank_?_!_new= rank_?_!_new+1;
		      retain rank_?_!_new;
	   	 run;

		 data &program.&levelletter._!_?;
		   set &program.&levelletter._!_?none (in=in1)
		   	   &program.&levelletter._!_?some (in=in2)
			   ;
			   if rank_?_!_new=. then rank_?_!_new=0;
			   if ?_!_c5wk=0 then rank_?_!_new = 0;
		  run;

        data curr_wk.&program.&levelletter._&mkt._!_?;
		   set &program.&levelletter._!_?;
		   keep id &terrdist. rank: ?_!shrchg_5wk ?_!chg_5wk;
		run;

		proc datasets nolist;
		delete &program.&levelletter._!_? &program.&levelletter._!_?some &program.&levelletter._!_?none;
		quit;
									  ),%str(!))
									  ),%str(?));

%mend;




****************************************;
* ADDRESS FILE FOR OUTPUT TO XL         ;
****************************************;


%macro addr(levelletter,terrdist);

%let addrvars = &terrdist. dmalertfirstcol physname addr city st zip 
	        tkspec tak_value phone allterrs  nat_rank terr_rank diabetes_seg 
		geo_footprint_desc terr_type_id call_priority repname TERR_TYPE_DESC_SHORT;



* get list of prods, mkts *;
data stringvar;
  set curr_wk.prodsmkt;
  newvar= left(trim(mkt))||"_"||left(trim(prod));
  newvar = compress(newvar);
  keep newvar;
run;

%msort(stringvar nodupkey,newvar);
run;

* make into a macro var for passing thru next macro;
proc sql noprint;
select newvar into :stringvar separated by ' '
from stringvar;
quit;

%put &stringvar.;

* create universe*;
data universe (keep = id &terrdist.);
	set %mscan(&stringvar.,%nrquote(
		%mscan(trx nrx, %nrquote(
			curr_wk.u&levelletter._?_!_rx
			curr_wk.u&levelletter._?_!_new
			curr_wk.u&levelletter._?_!_DHCP
			curr_wk.u&levelletter._?_!_GHCP
				),%str(!))
				),%str(?));
run;

%msort(universe nodupkey,id &terrdist.);
run;

%mformat(adw.terr_type_desc,terr_type_id,terr_type_desc_short,desc);
run;


	* gets the addresses for the DRILL IN Page;
	data &program. (keep = id &addrvars. &deciles.);
	set curr_wk.w2phyfinal&terrdist.
	   ;
	   terr_type_desc_short = put(terr_type_id,$desc.);
	   physname = upcase(left(trim(lname)))||", "||
			      upcase(left(trim(fname))) ||" "|| 
				  upcase(left(trim(mname)));
	run;

	%msort(&program.,id &terrdist.);

	*merge with universe*;

	data &program.;
		merge &program. (in=in1)
		      universe  (in=in2)
			; by id &terrdist.;
		if in1 and in2;
	run;
	

* NEW - SEPT 30 2010: PDRP CHECK;

* VERIFY NO PDRP;
data restricted_prof;
  set adw.restricted_prof;
if pdrp = 1;
run;

%mformat(restricted_prof,id,id,pdrp);
run;

data chk;
  set &program.;
  if put(id,$pdrp.)>"" then abort;
run;
	   	
	data &program. (drop=phone2);
	  set &program. (rename =(phone=phone2));
	  	if phone2 ~="" then do;
			phone=substr(phone2,1,3)||"-"||substr(phone2,4,3)||"-"||substr(phone2,7,4);
		end;
		tablekey = 'addr';
	run;
	
	%msort(&program.,&terrdist. id);
	run;

	data &program. (rename = (id = joinkey));
		set &program.;
		drillddn = trim(physname) ||" (" || trim(id) || ")";
	run;

%let retainvars = &terrdist. joinkey physname addr city st zip 
	        tkspec tak_value phone allterrs  nat_rank terr_rank diabetes_seg 
		geo_footprint_desc terr_type_id call_priority repname TERR_TYPE_DESC_SHORT
		&deciles.
		 drillddn;
 
	data &program.;
   		retain &retainvars.;
		set &program.;
	run;

	%msort(&program.,physname);
	run;
	
	*Terr List;
	%msort(curr_wk.sales_org nodupkey,&terrdist.,terrs);
	run;

	data terrlist1;
	  set terrs;
	if _N_ <1000;
	run;

	proc sql noprint;
		select distinct &terrdist. into :terrstring1 separated by ' '
		from terrlist1;
	quit;

	%put &terrstring1.;

	%if &terrdist.=terr %then %do;
	data terrlist2;
	  set terrs;
	  if _N_>=1000;
	run;

	proc sql noprint;
		select distinct &terrdist. into :terrstring2 separated by ' '
		from terrlist2;
	quit;
	%put &terrstring2.;
	%end;
	         

	%LET TEXTVARS=  &terrdist. joinkey physname addr city st zip 
			tak_value phone allterrs drillddn tkspec tak_value phone allterrs  
			 diabetes_seg 
			geo_footprint_desc terr_type_id call_priority repname TERR_TYPE_DESC_SHORT;

	%LET NUMVARS= &deciles. nat_rank terr_rank;


	DATA EMPTY;
	%MSCAN(&TEXTVARS.,%nrquote(
		!="";));

	%MSCAN(&NUMVARS.,%nrquote(
		!=.;));
	OUTPUT EMPTY;
	RUN;
*** FIRST SET OF GEOGRAPHIES ***;
	data %mscan(&terrstring1.,%nrquote(file_! ));
		set &program. (in=in1 rename = (&terrdist. = geo))
			empty 		   (in=in2 rename = (&terrdist. = geo));
		%mscan(&terrstring1.,%nrquote(
		if geo = "!" then output file_!;
		if in2 then output file_!;
			));
	run;


	%mscan(&terrstring1.,%nrquote(

	ods listing close;
	ods csv file = "&csvout./&terrdist./weekly_phyaddr_&currwknum._!.csv";
		proc print data = file_!;
	run;
	ods csv close;
	ods listing;

	proc datasets nolist;
		delete file_!;
	quit;

	));
*** SECOND SET OF GEOGRAPHIES ***;
	%if &terrdist.=terr %then %do;
	data %mscan(&terrstring2.,%nrquote(file_! ));
		set &program. (in=in1 rename = (&terrdist. = geo))
			empty 		   (in=in2 rename = (&terrdist. = geo));
		%mscan(&terrstring2.,%nrquote(
		if geo = "!" then output file_!;
		if in2 then output file_!;
			));
	run;


	%mscan(&terrstring2.,%nrquote(

	ods listing close;
	ods csv file = "&csvout./&terrdist./weekly_phyaddr_&currwknum._!.csv";
		proc print data = file_!;
	run;
	ods csv close;
	ods listing;

	proc datasets nolist;
		delete file_!;
	quit;

	));
%end;

	proc datasets nolist;
		delete &program.;
	quit;

	run;

%mend;