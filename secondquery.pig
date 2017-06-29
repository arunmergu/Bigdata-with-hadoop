loadingdata = load '/home/hduser/dataforjava' using PigStorage(',') as (age : int,eductaion : chararray,martialstatus : chararray,gender : chararray,taxfilerstatus : chararray,income : double,parents : chararray,countryofbirth : chararray,citizenship : chararray,weekworked : int);
gengroup = group loadingdata all;
--for caluculationg maximum value of weeks
maximumweek = foreach gengroup generate MAX(loadingdata.weekworked) as maximum;
selectingpeople = filter loadingdata by weekworked==maximumweek.maximum;
incomeofmostworked = foreach selectingpeople generate income; 
--group for 
caluclationofavg = group incomeofmostworked all;
--average of final
finalavg = foreach caluclationofavg generate AVG(incomeofmostworked.income) as averagefinal;
--income of top 5
orderingincome = order loadingdata by income desc;
limitingto5 = limit orderingincome 5;
forraverage = group limitingto5 all;
finnalaverage = foreach forraverage generate AVG(limitingto5.income) as averageoftop5;
--diff of their averages
differenceofavg = foreach finnalaverage generate (finnalaverage.averageoftop5-finalavg.averagefinal)*100/finalavg.averagefinal as difference;
