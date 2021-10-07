%% +---------------------------------------------------------------------------+
%% | File:         bank_app.erl
%% +---------------------------------------------------------------------------+
%% | Author:       Romin Per
%% +---------------------------------------------------------------------------+
%% | Description:
%% | Implementation of a bank database in mnesia and some tools to
%% | work with the data.
%% |
%% | start erlang with > erl -mnesia dir '"/tmp/mnesia"'
%% |
%% | Compile the file with > c(bank_app). 
%% | start mnesia database including the data from the file banklist.csv
%% | with > bank_app:create_banklist(). it will return the number of posts.
%% | 
%% | the goal:
%% | The function > bank_app:get_bank_dates_per_state("FL"). will give the
%% | oldest date and the newest date for the state Florida.
%% |
%% | The function > bank_app:get_bank_between_dates("7-Sep-11","23-Sep-13","FL").
%% | 
%% | some extra functions:
%% | get_banks/0 will get the first bank in the list
%% | get_banks_in_state/1 will get all the banks in a state with closing date
%% |
%% | get_bank_state/1 will get the state from no
%% | get_bank_name/1 will get the bank name from no
%% | get_bank_cert_no/1  will get the cert number from no
%% | get_bankinfo_cert_no/1 will get some info about the CERT number
%% |
%% | get_bank_closing_date/1 will get at touple with the closing date from no
%% | get_bank_updated_date/1 will get at touple with the updated date from no
%% |
%% | find_low_no/1 will list all banks with a a number less or equal to no
%% | convert_date/1 will convert the date as a string to a touple "28-Apr-17" -> {2017,4,28}
%% |
%% +---------------------------------------------------------------------------+
-module(bank_app).
-export([start/1,create_banklist/0]).
-export([get_banks/0,get_banks_in_state/1,get_bank_state/1,get_bank_name/1]).
-export([get_bank_cert_no/1,get_bankinfo_cert_no/1,get_bank_closing_date/1,get_bank_updated_date/1,get_bank_between_dates/3,get_bank_dates_per_state/1]).
-export([find_low_no/1,convert_date/1]).

%%application:set_env(mnesia, dir, "/tmp/mnesia").


%Bank Name,City,ST,CERT,Acquiring Institution,Closing Date,Updated Date
-record(banks, {no,
	       bank_name=[],
	       city=[],
	       state=[],
	       cert_no=[],
	       acquiring_inst=[],
	       closing_date=[],
	       updated_date=[]}).

start_mnesia() ->
    mnesia:create_schema([node()]),
    mnesia:start().
stop_mnesia() ->
    mnesia:stop().
delete_mnesia() ->
    mnesia:delete_schema([node()]).

writeto_mnesia(Bank_name) ->
    Bank_name#banks.no,
    mnesia:transaction(fun() ->
        mnesia:write(Bank_name)
    end).

start(Filename) ->
    {ok, Pid} = file:open(Filename,[read]),
    Pid.

store_banks([],Bank_name,N) ->
    {Bank_name,N};
store_banks([H|String],Bank_name,N) ->
    N1 = N + 1,
    [B,C,S,Ce,Ac,Cl,Up|T] = string:tokens(H, ","),
    [B1,C1,S1,Ce1,Ac1,Cl1,Up1|_T1] =
        case string:chr(Cl, $") of
	     0 -> 
		     case string:chr(S, $") of
    	     	   	0 ->
			   case string:chr(C, $") of
    	     	   	      0 ->
			         case string:chr(Up, $") of
    	     	   	      	    0 -> [B,C,S,Ce,Ac,Cl,Up|T];
			      	       _N1 -> 
			                  [B,C,S,Ce,string:concat(Ac,string:concat(Cl,Up))|T]
		     	   	 end;
			      _N2-> 
			     	 [string:concat(B,C),S,Ce,Ac,Cl,Up|T]
			   end;
			_N3 -> 
			     [string:concat(B,string:concat(C,S)),Ce,Ac,Cl,Up|T]
		     end;
	     _N4 -> 
		     case string:tokens(H, ",") of
    	     	   	[B12,C12,S12,Ce12,Ac12,Cl12,Up12,Extr12,Extr22|T12] ->  
			        [string:concat(B12, C12),S12,Ce12,Ac12,string:concat(Cl12,Up12),Extr12,Extr22|T12];
    	     	   	[B12,C12,S12,Ce12,Ac12,Cl12,Up12,Extr12|T12] -> 
			        [B12,C12,S12,Ce12,string:concat(Ac12,Cl12),Up12,Extr12|T12]
		     end
        end,
    NewBank_name = Bank_name#banks{no=N1,
		bank_name=B1,
	       	city=C1,
	       	state=S1,
	       	cert_no=Ce1,
	       	acquiring_inst=Ac1,
	       	closing_date=Cl1,
	       	updated_date=Up1},
    writeto_mnesia(NewBank_name),
    store_banks(String,NewBank_name,N1).


create_banklist() ->
    stop_mnesia(),
    delete_mnesia(),
    start_mnesia(),
    {ok, File_pid} = file:open("banklist.csv",[read]),
    {ok, String} = file:read(File_pid, 100000),
    [Bank|T] = string:tokens(String, "\n"),
    [B,C,S,Ce,Ac,Cl,Up|_T] = string:tokens(Bank, ","),
    Bank_name = #banks{no=0,
		bank_name=B,
	       	city=C,
	       	state=S,
	       	cert_no=Ce,
	       	acquiring_inst=Ac,
	       	closing_date=Cl,
	       	updated_date=Up},

    mnesia:create_table(banks, [{attributes, record_info(fields, banks)}]),
    {_NewBank_name,N} = store_banks(T,Bank_name,0),
    N.

get_banks() ->
    F = fun() -> mnesia:match_object(#banks{no = 1, _ = '_'}) end,
    {atomic,[Bank_name]} = mnesia:transaction(F),
    Bank_name#banks.bank_name.

get_bank_name(No) when is_integer(No) ->
    Banks = #banks{no = No, _ = '_'},
    F = fun() -> mnesia:match_object(Banks) end,
    {atomic,[Bank_name]} = mnesia:transaction(F),
    Bank_name.

get_banks_in_state(St) ->
    Banks = #banks{state = St, _ = '_'},
    F = fun() -> mnesia:match_object(Banks) end,
    {atomic,[Bank_name|String]} = mnesia:transaction(F),   	   
    print_banks([Bank_name|String]).

get_bank_state(No) when is_integer(No) ->
    Banks = #banks{no = No, _ = '_'},
    F = fun() -> mnesia:match_object(Banks) end,
    {atomic,[Bank_name]} = mnesia:transaction(F),   	   
    Bank_name#banks.state.

get_bank_cert_no(No) when is_integer(No) ->
    Banks = #banks{no = No, _ = '_'},
    F = fun() -> mnesia:match_object(Banks) end,
    {atomic,[Bank_name]} = mnesia:transaction(F),   	   
    Bank_name#banks.cert_no.

get_bankinfo_cert_no(Ce) ->
    Banks = #banks{cert_no = Ce, _ = '_'},
    F = fun() -> mnesia:match_object(Banks) end,
    {atomic,[Bank_name]} = mnesia:transaction(F),
    {Bank_name#banks.no,Bank_name#banks.bank_name,Bank_name#banks.closing_date}.

get_bank_closing_date(No) when is_integer(No) ->
    Banks = #banks{no = No, _ = '_'},
    F = fun() -> mnesia:match_object(Banks) end,
    {atomic,[Bank_name]} = mnesia:transaction(F),
    String = Bank_name#banks.closing_date,
    convert_date(String).

get_bank_updated_date(No) when is_integer(No) ->
    Banks = #banks{no = No, _ = '_'},
    F = fun() -> mnesia:match_object(Banks) end,
    {atomic,[Bank_name]} = mnesia:transaction(F),
    String = Bank_name#banks.updated_date,
    convert_date(String).

get_bank_between_dates(Date1,Date2,St) ->
    Banks = #banks{state = St, _ = '_'},
    F = fun() -> mnesia:match_object(Banks) end,
    {atomic,[Bank_name|String]} = mnesia:transaction(F),   	   
    check_date(Date1,Date2,[Bank_name|String]).

get_bank_dates_per_state(St) ->
    Banks = #banks{state = St, _ = '_'},
    F = fun() -> mnesia:match_object(Banks) end,
    {atomic,[Bank_name|String]} = mnesia:transaction(F),   	   
    Lowest_date = get_low_date(String,Bank_name#banks.closing_date),
    Highest_date = get_high_date(String,Bank_name#banks.closing_date),
    io:fwrite("~p, ~p, ~p",[Lowest_date,Highest_date,St]).

get_low_date([],Date)->Date;
get_low_date([Bank_name|String],Date)->
    {Year,Month,Day} = convert_date(Date),    
    {YearH,MonthH,DayH} = convert_date(Bank_name#banks.closing_date),    
    NewDate =
       case YearH of
          Y when Y > Year-> Date;
          Y when Y < Year-> Bank_name#banks.closing_date;
          Y when Y == Year->
          case MonthH of
              M when M > Month -> Date;
              M when M < Month -> Bank_name#banks.closing_date;
              M when M == Month ->
              case DayH of
                  D when D > Day -> Date;
                  D when D < Day -> Bank_name#banks.closing_date;
                  D when D == Day -> Date
    	      end
          end
       end,
    get_low_date(String,NewDate).

get_high_date([],Date)->Date;
get_high_date([Bank_name|String],Date)->
    {Year,Month,Day} = convert_date(Date),    
    {YearH,MonthH,DayH} = convert_date(Bank_name#banks.closing_date),    
    NewDate =
       case YearH of
          Y when Y < Year-> Date;
          Y when Y > Year-> Bank_name#banks.closing_date;
          Y when Y == Year->
          case MonthH of
              M when M < Month -> Date;
              M when M > Month -> Bank_name#banks.closing_date;
              M when M == Month ->
              case DayH of
                  D when D < Day -> Date;
                  D when D > Day -> Bank_name#banks.closing_date;
                  D when D == Day -> Date
    	      end
          end
       end,
    get_high_date(String,NewDate).

check_date(_Date1,_Date2,[])-> {};
check_date(Date1,Date2,[Bank_name|String])->
    {Year1,Month1,Day1} = convert_date(Date1),
    {Year2,Month2,Day2} = convert_date(Date2),
    {Year,Month,Day} = convert_date(Bank_name#banks.closing_date),
    case Year of
       Y when Y < Year1 -> {};
       Y when Y > Year2 -> {};
       Y when Y == Year2 ->    
         case Month of
	     M when M > Month2 -> {};
	     M when M < Month2 ->
	         print_banks([Bank_name]);		 
       	     M when M ==  Month2 ->
                 case Day of
	             D when D > Day2 -> {};
        	     D when D < Day2 ->
	               print_banks([Bank_name]);
        	     D when D == Day2 ->
     	               print_banks([Bank_name])
		 end
	 end;
       Y when Y ==  Year1 ->
         case Month of
	     M when M < Month1 -> {};
	     M when M > Month1 ->
	         print_banks([Bank_name]);		 
       	     M when M ==  Month1 ->
                 case Day of
	             D when D < Day1 -> {};
        	     D when D > Day1 ->
	               print_banks([Bank_name]);
		     D when D ==  Day1 ->
	               print_banks([Bank_name])
		 end
	 end;
       Y when Y >  Year1 ->
         print_banks([Bank_name])
    end,
    check_date(Date1,Date2,String).

print_banks([]) -> ok;
print_banks([NewBank_name|String]) ->
    io:fwrite(NewBank_name#banks.bank_name),
    io:fwrite(", "),
    io:fwrite(NewBank_name#banks.closing_date),
    io:fwrite("\n"),
    print_banks(String).


find_low_no(No) ->
        Constraint = 
             fun(Bnk, Acc) when Bnk#banks.no =< No ->
                    [Bnk | Acc];
                (_, Acc) ->
                    Acc
             end,
        Find = fun() -> mnesia:foldl(Constraint, [], banks) end,
        mnesia:transaction(Find).

convert_date([D1,$-,M1,M2,M3,_,Y1,Y2|String]) ->
    convert_date([$0,D1,$-,M1,M2,M3,$-,Y1,Y2|String]);
convert_date([D1,D2,_,M1,M2,M3,_,Y1,Y2|_String]) ->
    DateInt = list_to_integer([D1,D2]),
    MontInt = month([M1,M2,M3]),
    YearInt = list_to_integer(year([Y1,Y2])),
    list_to_tuple([YearInt,MontInt,DateInt]).

year(Y)-> string:concat("20",Y).
month("Jan")->1;
month("Feb")->2;
month("Mar")->3;
month("Apr")->4;
month("May")->5;
month("Jun")->6;
month("Jul")->7;
month("Aug")->8;
month("Sep")->9;
month("Oct")->10;
month("Nov")->11;
month("Dec")->12.
