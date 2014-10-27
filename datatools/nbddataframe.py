import pandas as pd
from sqlalchemy import create_engine, MetaData
from StringIO import StringIO
from mpl_toolkits.basemap import Basemap
import matplotlib.pyplot as plt

pd.options.display.mpl_style = 'default'


#data string for testing purposes
testdata=\
"""\
val	lat	lon	rand	neighborhood	date
4.0764441201	47.600025185	-122.373927686	0.1276587849	B	1/1/2000
4.2520509753	47.4	-122.3413044916	0.8755922036	A	6/20/1986
1.3226973685	47.634555916	-122.3442923659	0.3654616242	B	10/13/1987
-9.3627561629	47.6509594572	-122.2993523445	0.0957500809	B	1/1/2000
	47.5445857933	-122.3436249441	0.1716343011	B	1/1/2000
-5.911734812	47.5112812526		0.0838911855	B	1/1/2000
4.7337248223	47.6178126252	-122.3997977501	0.9184924145	A	6/20/1986
-9.5657038502	47.5267191329	-122.1	0.2555611331	B	1/1/2000
-6.5947027411	47.72339967	-122.3703101539	0.3693092365	B	10/13/1987
3.5680261068	47.7431436405	-122.3081758471	0.6814667757	A	6/20/1986
-0.4510013713	47.7208316866	-122.2663515676	0.9224957069	A	6/20/1986
	47.5186485248	-122.3297974329	0.4966851496	B	10/13/1987
-6.0590357706	47.5794698747	-122.4396772129	0.5864697497	A	10/13/1987
"""

#lat long bounds
minlat = 47.5
"""The default minimum latitude."""

maxlat = 47.75
"""The default maximum latitude."""

minlong = -122.44
"""The default minimum longitude."""

maxlong = -122.2
"""The default maximum longitude."""

def rename_cols(df, nbdname=None, latname='latitude',
                longname='longitude', datename='date'):
    """
    Rename the longitude, latitude, date (and convert to time type)
    and nbd (if there is one) columns to make the DataFrame *df* 
    compatible with :class:`NBDDataFrame`.
    
    Parameters:
    ___________
    
    :param str nbdname: 
        (optional) the name of the neighborhood column
        if there is one, default is None
    
    :param str latname: 
        the name of the latitude column, default is 'latitude'
    
    :param str longname: 
        the name of the longitude column, default is 'longitude'
    
    :param str datename: 
        the name of the date column, default is 'date'
        
    Returns:
    ________
    
    :return: a pandas.DataFrame compatible with :class:`NBDDataFrame`
    :rtype: pandas.DataFrame

    """     
    
    newdf = df.rename(columns={latname:'latitude', longname:'longitude',
                       datename:'date'})
    if nbdname:
        newdf.rename(columns={nbdname:'nbd'}, inplace=True)
    
    newdf['date'] = pd.to_datetime(newdf['date'])    
           
    return newdf
    


def get_csv_data(filename, nbdname=None, latname='latitude',
                 longname='longitude', datename='date', sep='\t'):
    """
    Read neighborhood data from a csv into a pandas.DataFrame
    compatible with :class:`NBDDataFrame`. The csv data should have at
    the least latitude and longitude columns with names *latname* and
    *longname*, and a date column with name *datename*. A neighborhood
    column with name *nbdname* is optional.
    
    Parameters:
    ___________
    
    :param str filename: 
        the name of the csv file
        
    :param str nbdname: 
        (optional) the name of the neighborhood column
        if there is one, default is None
    
    :param str latname: 
        the name of the latitude column, default is 'latitude'
    
    :param str longname: 
        the name of the longitude column, default is 'longitude'
    
    :param str datename: 
        the name of the date column, default is 'date'
    
    :param str sep: 
        the separation character, default is tab
    
    Returns:
    ________
    
    :return: a pandas.DataFrame compatible with :class:`NBDDataFrame`
    :rtype: pandas.DataFrame
    
    For example,
    
    >>> from datatools.nbddataframe import testdata, get_csv_data
    >>> fil = open('test.csv', 'w') #make a test csv file
    >>> fil.write(testdata) #write data randomly generated for test purposes 
    >>> fil.close()
    >>> df = get_csv_data(
    ...                   filename='test.csv', nbdname='neighborhood',
    ...                   latname='lat', longname='lon', datename='date', 
    ...                   sep='\t'
    ... )
    >>> df.head()
            val   latitude   longitude      rand nbd       date
    0  4.076444  47.600025 -122.373928  0.127659   B 2000-01-01
    1  4.252051  47.400000 -122.341304  0.875592   A 1986-06-20
    2  1.322697  47.634556 -122.344292  0.365462   B 1987-10-13
    3 -9.362756  47.650959 -122.299352  0.095750   B 2000-01-01
    4       NaN  47.544586 -122.343625  0.171634   B 2000-01-01
    >>> df.loc[0, 'nbd']
    'B'
    >>> df.mean().loc['latitude']
    47.597802519907695
        
    """

    df = pd.read_csv(filename, sep='\t', parse_dates=[datename])
    df.rename(columns={latname:'latitude', longname:'longitude',
                       datename:'date'}, inplace=True)
    if nbdname:
        df.rename(columns={nbdname:'nbd'}, inplace=True)
    
    return df
    
    
#test dataframe
testdataframe = get_csv_data(
                  filename=StringIO(testdata), nbdname='neighborhood',
                  latname='lat', longname='lon', datename='date', sep='\t'
)


def get_db_data(engine, tablename='nbddata', nbdname=None, 
                latname='latitude', longname='longitude', datename='date',
                index_col=None):
    """
    Read neighborhood data from a database into a pandas.DataFrame 
    compatible with :class:`NBDDataFrame`. The table should have at the least 
    latitude and longitude columns with names *latname* and *longname*, and a 
    date column with name *datename*. A neighborhood column with name 
    *nbdname* is optional.
    
    Parameters:
    ___________
    
    :param sqlalchemy.engine.Engine engine: 
        the database engine
        
    :param str tablename: 
        the name of the database table, default is 'nbddata'
        
    :param str nbdname: 
        (optional) the name of the neighborhood column if
        there is one, default is None
        
    :param str latname: 
        the name of the latitude column, default is 'latitude'
        
    :param str longname: 
        the name of the longitude column, default is 'longitude'
        
    :param str datename: 
        the name of the date column, default is 'date'
        
    :param str index_col:
        the name of the index column if there is one, default is None 
    
    Returns:
    ________
    
    :return: a DataFrame compatible with :class:`NBDDataFrame`
    :rtype: pandas.DataFrame
    
    For example,

    >>> from datatools.nbddataframe import testdataframe, NBDDataFrame
    >>> from datatools.nbddataframe import make_db, get_db_data
    >>> neigh_dataframe = NBDDataFrame(testdataframe)
    >>> engine = make_db(nbddf=neigh_dataframe, tablename='neigh_data')
    >>> df2 = get_db_data(engine=engine, tablename='neigh_data', 
    ...                   nbdname='nbd', latname='latitude', 
    ...                   longname='longitude', datename='date'
    ... )
    >>> df2.loc[1, 'nbd']
    u'A'
    
    """
        
    df = pd.read_sql_table(table_name=tablename, con=engine,
                           parse_dates=[datename], index_col=index_col)
    colrndict = {latname:'latitude', longname:'longitude', datename:'date'}
    df.rename(columns=colrndict, inplace=True)
    if nbdname:
        df.rename(columns={nbdname:'nbd'}, inplace=True)
    
    return df
    
    
def make_db(nbddf, dbname=None, tablename='nbddata'):
    """
    Write :class:`NBDDataFrame` data into a SQLite database.
    
    Parameters:
    ___________
    
    :param NBDDataFrame nbddf: 
        the :class:`NBDDataFrame` object containing the data
        
    :param str dbname: 
        the name of the database; if no name is given, 
        the database will be in-memory-only, default is None
        
    :param str tablename: 
        the name of the table, default is 'nbddata'
    
    Returns:
    ________
    
    :returns: an engine to the database
    :rtype: sqlalchemy.engine.Engine
    
    For example,

    >>> from datatools.nbddataframe import testdataframe, NBDDataFrame
    >>> from datatools.nbddataframe import make_db
    >>> neigh_dataframe = NBDDataFrame(testdataframe)
    >>> engine = make_db(nbddf=neigh_dataframe, tablename='neigh_data')
    >>> con = engine.connect()
    >>> res = con.execute("select latitude from neigh_data where nbd = 'A'")
    >>> data = res.fetchall()
    >>> data[0][0]
    47.4
    >>> con.close()
    
    """
    #make the engine
    if dbname is None:
        engine = create_engine('sqlite://')
    else:
        engine = create_engine('sqlite:///{}.db'.format(dbname))
    
    nbddf.get_df().to_sql(name=tablename, con=engine)
    return engine    
       

class NBDDataFrame(object):
    """
    A neighborhood data cleaner and preliminary analyzer.
    
    Parameters:
    ___________
    
    :param pandas.DataFrame df: 
        the neighborhood data: at the least,
        it should have *latitude*, *longitude* and *date* columns
        
    :param float min_lat:
        the minimum considered latitude, default is :attr:`minlat`

    :param float max_lat:
        the maximum considered latitude, default is :attr:`maxlat`

    :param float min_long:
        the minimum considered longitude, default is :attr:`minlong`

    :param float max_long:
        the maximum considered longitude, default is :attr:`maxlong`
        
    :param bool debug:
        if True, produce verbose output; default is False
        
    Raises:
    _______
    
    :raises Exception: if *df* does not have the required columns
    
    For example, we read a test DataFrame into an :class:`NBDDataFrame`.
        
    >>> from datatools.nbddataframe import testdataframe, NBDDataFrame
    >>> nbddf = NBDDataFrame(testdataframe, debug=True)
    The DataFrame is in the correct format
    
    An exception is raised if the DataFrame is not in the correct format
    (in this case, a *date* column is missing). 
    
    >>> nbddf = NBDDataFrame(testdataframe[['latitude', 'longitude']])
    Traceback (most recent call last):
        ...
    Exception: DataFrame format error

    To print missing data info,
    
    >>> nbddf = NBDDataFrame(testdataframe)
    >>> print nbddf.print_info()
    The number of rows is 13.
    2 rows are missing val.
    1 rows are missing longitude.
    2 rows have out-of-bounds location.
    
    To remove rows with missing location or date data,
     
    >>> nbddf.remove_missing_data()
    >>> print nbddf.print_info()
    The number of rows is 12.
    2 rows are missing val.
    2 rows have out-of-bounds location.
    
    To remove rows with out-of-bounds locations,
    
    >>> nbddf.remove_outofbounds_data()
    >>> print nbddf.print_info()
    The number of rows is 10.
    2 rows are missing val.
    
    Preliminary plots:
    
    >>> nbddf.plot_rowcount_by_month()
    >>> nbddf.plot_map()

    
    .. todo::
       * function to add neighborhoods
       * cleaning functions
       * grouping functions
       * vis/analysis functions
       * normalization functions
       * Use shapefiles to get nbd instead of nearest-nbr predictor.          
    
    """
    
    def __init__(self, df, min_lat=minlat, max_lat=maxlat, min_long=minlong, 
                 max_long=maxlong, debug=False):

        self.df = df
        
        required_cloumns = set(['latitude', 'longitude', 'date'])
        if not required_cloumns.issubset(df.columns):
            raise Exception('DataFrame format error')
        elif debug:
            print "The DataFrame is in the correct format"
            
        self.min_lat = min_lat
        self.max_lat = max_lat
        self.min_long = min_long
        self.max_long = max_long

        self.seattlemap = None            
            
    
    def print_info(self):
        """
        Print the number of rows, number of missing values for each column,
        and number of rows with location out of bounds.
        
        Returns:
        ________
        
        :returns: the above information
        :rtype: str
        
        """
        df = self.get_df()
        
        count = df.count()
        num_rows = len(df)
        printstr = "The number of rows is {}.\n".format(num_rows)
        missing_count = num_rows - count
        for col in missing_count.index:
            if missing_count[col] > 0:
                missstr = "{num} rows are missing {colu}.\n"
                printstr += missstr.format(num=missing_count[col], colu=col)

        out_of_bounds = len(df[(df.latitude < self.min_lat) | 
                               (df.latitude > self.max_lat) |
                               (df.longitude < self.min_long) | 
                               (df.longitude > self.max_long)]
        )

        if out_of_bounds > 0:
            ofbstr = "{num} rows have out-of-bounds location.\n"
            printstr += ofbstr.format(num=out_of_bounds)                
                
        return printstr[:-1]
                
    def remove_missing_data(self):
        """
        Remove rows with missing location or date data.
        
        """
        
        self.df.replace({'latitude' : 0, 'longitude' : 0}, None)
        self.df = self.df[(self.df.date.notnull()) & 
                          (self.df.latitude.notnull()) & 
                          (self.df.longitude.notnull())
        ]
        
    def remove_outofbounds_data(self):
        """
        Remove rows with out-of-bounds locations.
        
        """
        
        self.df = self.df[(self.df.latitude >= self.min_lat) & 
                          (self.df.latitude <= self.max_lat) & 
                          (self.df.longitude >= self.min_long) & 
                          (self.df.longitude <= self.max_long)
        ]
            
   
    def plot_rowcount_by_month(self, df=None,
                               filename="rowcount_by_month.png"):
        """
        Plot the number of rows by month.
        
        Parameters:
        ___________
        
        :param DataFrame df:
            the data to plot: if None, 
            then this object's underlying DataFrame is used, 
            default is None
        
        :param str filename: 
            the name of the file with the plot, 
            default is "rowcount_by_month.png"
        
        """
        
        if df is None:
            df = self.get_df()
            
        ax = plt.subplot(111)
        numrowsbydate = df[['date']].groupby(df.date).count()
        resamplebymonth = numrowsbydate.resample("M", how="sum")
        ax.plot(resamplebymonth.index, resamplebymonth)
        ax.set_title("Row count by month")
        plt.savefig(filename, dpi=200)
        plt.clf()
        
        
    def plot_map(self, df=None, filename="row_locations_map.png"):
        """
        Plot the number of rows by month.
        
        Parameters:
        ___________
        
        :param DataFrame df:
            the data to plot: if None, 
            then this object's underlying DataFrame is used, 
            default is None
        
        :param str filename: 
            the name of the file with the plot, 
            default is "rowcount_by_month.png"
        
        """

        if df is None:
            df = self.get_df()
        
        if self.seattlemap is None:    
            self.setup_map()    
        
        locax = plt.subplot(111)                              
        self.seattlemap.drawcoastlines(ax=locax)
        locax.scatter(df.longitude, df.latitude, s=8, marker='.')
        locax.set_title("Locations")
        plt.savefig(filename, dpi=200)
        plt.clf()
                     
                
    def setup_map(self):
    
        self.seattlemap = Basemap(llcrnrlon = self.min_long, 
                                  llcrnrlat = self.min_lat, 
                                  urcrnrlon = self.max_long, 
                                  urcrnrlat = self.max_lat, 
                                  resolution='i')
                
    def get_df(self):
        """
        Get the underlying DataFrame.
        
        Returns:
        ________
        
        :returns: the underlying DataFrame
        :rtype: pandas.DataFrame
        
        """
        
        return self.df
