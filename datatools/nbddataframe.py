import pandas as pd
from sqlalchemy import create_engine, MetaData

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
"""

def get_csv_data(filename, nbdname=None, latname='latitude', longname='longitude', datename='date', sep='\t'):
    """
    Read neighborhood data from a csv into a DataFrame compatible with :class:`NBDDataFrame`. The csv data should have at the least latitude and longitude columns with names *latname* and *longname*, and a date column with name *datename*. A neighborhood column with name *nbdname* is optional.
    
    Parameters:
    ___________
    
    :param str filename: the name of the csv file
    :param str nbdname: (optional) the name of the neighborhood column is there is one, default is None
    :param str latname: the name of the latitude column, default is 'latitude'
    :param str longname: the name of the longitude column, default is 'longitude'
    :param str datename: the name of the date column, default is 'date'
    :param str sep: the separation character, default is tab
    
    Returns:
    ________
    
    :return: a DataFrame compatible with :class:`NBDDataFrame`
    :rtype: DataFrame
    
    For example,
    
    >>> from datatools.nbddataframe import testdata, get_csv_data
    >>> fil = open('test.csv', 'w') #make a test csv file
    >>> fil.write(testdata) #write data randomly generated for test purposes 
    >>> fil.close()
    >>> df = get_csv_data(filename='test.csv', nbdname='neighborhood', latname='lat', longname='lon', datename='date', sep='\t')
    >>> df.head()
            val  latitude  longitude      rand nbd       date
    0  4.076444 -0.967943  -0.618529  0.127659   B 2000-01-01
    1  4.252051  3.553520   5.001326  0.875592   A 1986-06-20
    2  1.322697 -3.535070  -0.303334  0.365462   B 1987-10-13
    3 -9.362756 -0.694518   9.115702  0.095750   B 2000-01-01
    4  7.277396  5.495874   1.116469  0.171634   B 2000-01-01
    >>> df.loc[0, 'nbd']
    'B'
    >>> df.mean().loc['latitude']
    -0.24481567373846144
        
    """

    df = pd.read_csv(filename, sep='\t', parse_dates=[datename])
    df.rename(columns={latname:'latitude', longname:'longitude', datename:'date'}, inplace=True)
    if nbdname:
        df.rename(columns={nbdname:'nbd'}, inplace=True)
    
    return df

def get_db_data(engine, tablename='data', nbdname=None, latname='latitude', longname='longitude', datename='date'):
    """
    Read neighborhood data from a database into a DataFrame compatible with :class:`NBDDataFrame`. The table should have at the least latitude and longitude columns with names *latname* and *longname*, and a date column with name *datename*. A neighborhood column with name *nbdname* is optional.
    
    Parameters:
    ___________
    
    :param sqlalchemy.engine.base.Engine engine: the database engine
    :param str tablename: the name of the database table, default is 'data'
    :param str nbdname: (optional) the name of the neighborhood column is there is one, default is None
    :param str latname: the name of the latitude column, default is 'latitude'
    :param str longname: the name of the longitude column, default is 'longitude'
    :param str datename: the name of the date column, default is 'date'
    
    Returns:
    ________
    
    :return: a DataFrame compatible with :class:`NBDDataFrame`
    :rtype: DataFrame
    
    """
        
    df = pd.read_sql_table(table_name=tablename, con=engine, parse_dates=[datename])
    df.rename(columns={latname:'latitude', longname:'longitude', datename:'date'}, inplace=True)
    if nbdname:
        df.rename(columns={nbdname:'nbd'}, inplace=True)
    
    return df
    
def make_db(nbddf, name=None):
    """
    Write :class:`NBDDataFrame` data into a SQLite database.
    
    Parameters:
    ___________
    
    :param NBDDataFrame nbddf: the :class:`NBDDataFrame` object containing the data
    :param str name: the name of the database; if no name is given, the database will be in-memory-only, default is None
    
    """
    #make the engine and connection
    if name is None:
        engine = create_engine('sqlite://')
    else:
        engine = create_engine('sqlite:///{}.db'.format(name))
    
    conn = engine.connect()   

class NBDDataFrame(object):
    """
    A neighborhood data cleaner and preliminary analyzer.
    
    Parameters:
    ___________
    
    :param Dataframe df: The neighborhood data. At the least, it should have *nbd*, *latitude*, *longitude* and *date* columns.
    
    """
    
    def __init__(self):
        pass
        
         
