import pandas as pd

#data for testing purposes
testdata=\
"""\
val	lat	lon	rand	neighborhood	date
4.0764441201	-0.9679426951	-0.6185285002	0.1276587849	B	1/1/2000
4.2520509753	3.5535197752	5.001326357	0.8755922036	A	6/20/1986
1.3226973685	-3.5350695765	-0.3033337044	0.3654616242	B	10/13/1987
-9.3627561629	-0.6945184339	9.1157019185	0.0957500809	B	1/1/2000
7.2773960605	5.4958742624	1.1164685618	0.1716343011	B	1/1/2000
-5.911734812	9.7288561845	-2.2021770012	0.0838911855	B	1/1/2000
4.7337248223	-8.3418089943	-0.3266175743	0.9184924145	A	6/20/1986
-9.5657038502	-3.7161306571	-7.3132784897	0.2555611331	B	1/1/2000
-6.5947027411	-2.9771319823	0.2907772316	0.3693092365	B	10/13/1987
3.5680261068	-1.1769245192	-6.236623642	0.6814667757	A	6/20/1986
-0.4510013713	7.2112883069	-3.3197442209	0.9224957069	A	6/20/1986
-1.4786966797	-8.637564606	-1.0909260344	0.4966851496	B	10/13/1987
-6.0590357706	0.8749491768	0.4776441725	0.5864697497	A	10/13/1987
-9.0557034221	-9.8762504151	-0.0184722943	0.5855213949	A	10/13/1987
1.0418056976	3.2738893712	-1.7843903555	0.7032793853	A	6/20/1986
9.3120111385	9.2805616744	3.1015036488	0.4999235398	B	10/13/1987
-5.4281971278	3.9662748063	3.8907729974	0.6531644475	A	10/13/1987
1.0093759513	-9.6628873236	-7.732220632	0.9217200647	A	6/20/1986
4.7918236535	-5.4886831669	-0.2459997917	0.0998065416	B	1/1/2000
"""

def get_csv_data(filename, nbdname, latname, longname, datename, sep='\t'):
    """
    Read neighborhood data from a csv and make a Dataframe compatible with :class:`NBDDataFrame`.
    
    Parameters:
    ___________
    
    :param str filename: the name of the csv file
    :param str nbdname: the name of the neighborhood column
    :param str latname: the name of the latitude column
    :param str longname: the name of the longitude column
    :param str datename: the name of the date column
    :param str sep: the separation character
    
    >>> fil = open('test.csv', 'w')
    >>> fil.write(testdata)
    >>> fil.close()
    >>> df = get_csv_data(filename='test.csv', nbdname='neighborhood', latname='lat', longname='lon', datename='date', sep='\t')
    >>> df.loc[0, 'nbd']
    'B'
    >>> df.mean().loc['latitude']
    -0.61524730591052623
        
    """

    df = pd.read_csv(filename, sep='\t', parse_dates=[datename])
    df.rename(columns={nbdname:'nbd', latname:'latitude', longname:'longitude', datename:'date'}, inplace=True)
    
    return df
    

class NBDDataFrame(object):
    """
    A neihborhood data cleaner and preliminary analyzer.
    
    Parameters:
    ___________
    
    :param Dataframe df: The neighborhood data. At the least, it should have *nbd*, *latitude*, *longitude* and *date* columns.
    
    """
    
    def __init__(self):
        pass
        
         
