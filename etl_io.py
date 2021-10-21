# -*- coding: utf-8 -*-
"""
Created on Thu Oct  7 10:51:55 2021

@author: mossg
"""
import sqlite3
import pandas as pd
import pypyodbc
import requests
from bs4 import BeautifulSoup
import os
import zipfile
import traceback
import ast

class IPEDSHandler():
    '''
    handles io with NCES IPEDS Access database
    
    NOTE: TABLE NAMES WILL CHANGE OVER TIME
    NOTE: TABLE STRUCTURE MAY CHANGE OVER TIME
    
    NCES does not provide a clean, agnostic version of the database, when applying
    to newer version of the database, after the 2019-2020 school year, code will
    need to be updated
    '''
    
    conn = None
    engine = None
    db = None
    tables = None
    metadata = None
    varnames = None
    
    def get_tables(self,name='tables19'):
        '''
        get list of tables
        
        Parameters
        ----------
        name : string, optional
            table containing list of tables in database. The default is 'tables19'.

        Returns
        -------
        tables : dataframe
            dataframe of table metadata
        '''
        try:
            return(pd.read_sql_query("SELECT TableName, TableTitle FROM {};".format(name), self.conn))
        except Exception as e:
            print(e)
            
    def get_varnames(self,name='vartable19'):
        '''
        get dict of var names to var titles by table

        Parameters
        ----------
        name : string, optional
            table of variables in database. The default is 'vartable19'.

        Returns
        -------
        varnames : dict
            dictionary containing variable names and titles by table

        '''
        
        try:
            values = pd.read_sql_query("SELECT TableName, varName, varTitle FROM {};".format(name), self.conn)
            
            varnames = {}
            for tab in values['TableName'].unique():
                temp = values[values['TableName']==tab][['varName','varTitle']].drop_duplicates()
                varnames[tab] = {}
                for index, row in temp.iterrows():
                    varnames[tab][row['varName']] = row['varTitle']
    
            return(varnames)
        except Exception as e:
            print(e)
        
    def get_metadata(self,name='valuesets19'):
        '''
        get metadata dictionary for variable encoding

        Parameters
        ----------
        name : string, optional
            table containing data values. The default is 'valuesets19'.

        Returns
        -------
        meta : dict
            dictionary containing data values

        '''
        
        try:
            values = pd.read_sql_query("SELECT * FROM {};".format(name), self.conn)
            
            meta = {}
            
            for var in values['varName'].unique():
                sub = values[values['varName']==var]
                meta[var] = dict(tuple(sub[['Codevalue','valueLabel']].values))
            
            return(meta)
        except Exception as e:
            print(e)
    
    def connect(self,
                engine = r"Driver={Microsoft Access Driver (*.mdb, *.accdb)};",
                db = r"Dbq=D:/data/IPEDS_2019-20_Provisional/IPEDS201920.accdb;"):
        '''
        connects to database
        Parameters
        ----------
        engine : string, optional
            database driver. The default is r"Driver={Microsoft Access Driver (*.mdb, *.accdb)};".
        db : string, optional
            database location. The default is r"Dbq=D:/data/IPEDS_2019-20_Provisional/IPEDS201920.accdb;".

        Returns
        -------
        None.
        '''
        self.engine = engine
        self.db = db
        
        #establish connection
        pypyodbc.lowercase = False
        self.conn = pypyodbc.connect(self.engine + self.db)
        self.tables = self.get_tables()
        self.metadata = self.get_metadata()
        self.varnames = self.get_varnames()
    
    def close(self):
        '''
        closes database
        '''
        try:
            self.conn.close()
        except Exception as e:
            print(e)
    
    def clean(self, df, name, replace=False, rename=False):
        if replace:
            df.replace(self.metadata,inplace=True)
        if rename:
            df.rename(columns=self.varnames[name],inplace=True)
        return(df)
        
    def select(self, name='', variables='*', where='', sql='', replace=False, rename=False):
        '''
        select data from table in database
        
        Parameters
        ----------
        conn : Connection
            database connection.
        name : string, optional
            table name. The default is ''.
        variables : list of strings, optional
            one or more variables to be queried from tabel. The default is '*'.
        where : string, optional
            optional where statement for querying specific data. The default is ''.
        sql : string, optional
            custom sql for extracting specific information. The default is ''.
        replace : bool, optional
            replace table values using metadata. The defualt is False
    
        Returns
        -------
        table : dataframe
            dataframe containing selected data
        '''
        
        try:
            if (name == '') and (sql == ''):
                return(None)
            if (where == '') and (sql == ''):
                table = pd.read_sql_query("SELECT {} FROM {};".format(','.join(variables),name), self.conn)                
                if replace or rename:
                    table = self.clean(table, name, replace, rename)
                return(table)
            elif (where != '') and (sql == ''):
                table = pd.read_sql_query("SELECT {} FROM {} WHERE {};".format(','.join(variables),name,where), self.conn)
                if replace or rename:
                    table = self.clean(table, name, replace, rename)
                return(table)
            table = pd.read_sql_query(sql, self.conn)
            name = sql.split('FROM ')[1].split()[0]
            if replace or rename:
                table = self.clean(table, name, replace, rename)
            return(table)
            
        except Exception as e:
            print(e)
        
    def to_sqlite3(self,target):
        '''
        converts access database to sqlite3 database format

        Parameters
        ----------
        target : string
            file path for new sqlite3 database.

        Returns
        -------
        None.
        '''
        
        conn_out = sqlite3.connect(target)
        for tab in self.tables['TableName']:
            self.select(tab).to_sql(tab,conn_out)
        
    def get_unitid(self,state_fips=None,county_fips=None,zipcode=None,clean_geography=False):
        '''
        get unit ids for colleges in specific states, counties, and / or zip codes

        Parameters
        ----------
        state_fips : int, or list of ints, optional
            state fips codes. The default is None.
        county_fips : int, or list of ints, optional
            county fips codes. The default is None.
        zipcode : int, or list of ints, optional
            zip codes. The default is None.
        clean_geography : bool, optional
            decode geography to human-readible sentences. The default is False

        Returns
        -------
        utd : list
            list of unit ids found in requested area

        '''
        if (state_fips==None) and (county_fips==None) and (zipcode==None):
            return()
        
        if state_fips!=None:
            try:
                state_where = ['FIPS IN ({})'.format(','.join([str(i) for i in state_fips]))]
            except Exception:
                state_where = ['FIPS IN ({})'.format(state_fips)]
        else:
            state_where = []

        if county_fips!=None:
            try:
                county_where = ['COUNTYCD IN ({})'.format(','.join([str(i) for i in county_fips]))]
            except Exception:
                county_where = ['COUNTYCD IN ({})'.format(county_fips)]
        else:
            county_where = []

        if zipcode!=None:
            try:
                zip_where = ['ZIP IN ({})'.format(','.join([str(i) for i in zipcode]))]
            except Exception:
                zip_where = ['ZIP IN ({})'.format(zipcode)]
        else:
            zip_where = []

        where = ' AND '.join(state_where + county_where + zip_where)
        utd = self.select(name='HD2019',variables=['UNITID','FIPS','STABBR','COUNTYCD','COUNTYNM','ZIP'],where=where)
        
        if clean_geography:
            utd['FIPS'] = utd['STABBR'].copy()
            utd['COUNTYCD'] = utd['COUNTYNM'] + ', ' + utd['STABBR']
        
        return(utd)
        
    def school_query(self,state_fips=None,county_fips=None,zipcode=None,cip_level=6,
               cipcode=[],majornum=1,unitid=[],how='total',rename=True,label=False,
               keep_geography=None, clean_geography=False):
        '''
        generate a table of raw, school-level data meeting specific requirements

        Parameters
        ----------
        state_fips : int, or list of ints, optional
            state fips codes. The default is None.
        county_fips : int, or list of ints, optional
            county fips codes. The default is None.
        zipcode : int, or list of ints, optional
            zip codes. The default is None.
        cip_level : int or string, optional
            desired cip level for output table
            options {
                6 : 6 digit cip codes,
                4 : 4 digit cip codes,
                2 : 2 digit cip codes,
                all : all cip codes,
                total : total awards
                }
            The default is 6.
        cipcode : list of string or int, optional
            list containing cip codes to be subset when querying data. The default is [], meaning all.
        majornum : int, optional
            first or second major. The default is 1.
        unitid : list, optional
            list of desired unitids. The default is [].
        how : string, optional
            how would you like the data tabulated    
            options {
                total : total awards by CIP, 
                race : awards by race and CIP, 
                sex : awards by sex and CIP, 
                race_sex : awards by race, sex, and CIP
                }
            The default is 'total'.
        rename : bool, optional
            rename columns to user-friendly output. The default is True.
        label : bool, optional
            add cip code lables. The default is False.
        keep_geography : str, optional
            keep geographic detail when aggregating
            options {
                None : no geographic data added
                FIPS : state geographic data added
                COUNTYCD : county geographic data added
                ZIP : zip code geographic data added
                }
            The default is None.
        clean_geography : bool, optional
            decode geography to human-readible sentences. The default is False

        Returns
        -------
        awd : dataframe
            dataframe containing school-level data.

        '''
        
        if (state_fips!=None) or (county_fips!=None) or (zipcode!=None):
            utd = self.get_unitid(state_fips, county_fips, zipcode, clean_geography)

            if len(utd) == 0:
                print('no colleges found in search area')
                return(None)
            
            if keep_geography != None:
                try:
                    geog = utd[['UNITID',keep_geography]]
                except Exception:
                    print('{} not valid for keep_geography. valid options: FIPS, COUNTYCD, ZIP')
                    return(None)
            
            unitid = list(utd['UNITID'])
            
        where = 'MAJORNUM = {}'.format(majornum)
        where += ' AND UNITID IN ({})'.format(', '.join([str(i) for i in unitid])) if len(unitid)>0 else ''
        
        if how=='total':
            variables = ['CTOTALT']
        elif how=='race':
            variables = ['CAIANT','CASIAT','CBKAAT','CHISPT','CNHPIT','CUNKNT','C2MORT','CWHITT']
        elif how=='sex':
            variables = ['CTOTALM','CTOTALW']
        elif how=='race_sex':
            variables = ['CAIANM','CASIAM','CBKAAM','CHISPM','CNHPIM','CUNKNM','C2MORM','CWHITM',
                         'CAIANF','CASIAF','CBKAAF','CHISPF','CNHPIF','CUNKNF','C2MORF','CWHITF']
        else:
            print('{} not a valid option for "how". valid options: total, race, sex, race_sex')
            return(None)
            
        awd = self.select(name='C2019_A',variables=['UNITID','CIPCODE']+variables,where=where)
        
        if cipcode != []:
            for i in range(len(cipcode)):
                front = '{:>02d}'.format(int(str(cipcode[i]).split('.')[0]))
                try:
                    back = '.' + str(cipcode[i]).split('.')[1]
                except Exception:
                    back = ''
                cipcode[i] = front + back
                
            awd = awd[awd['CIPCODE'].isin(cipcode)]
        
        if keep_geography != None:
            awd = awd.merge(geog,on='UNITID')
        
        return(awd)
    
    def awards(self,state_fips=None,county_fips=None,zipcode=None,cip_level=6,
               cipcode=[],majornum=1,unitid=[],how='total',rename=True,label=False,
               keep_geography=None,clean_geography=False):
        '''
        generate a table for awards by cip code

        Parameters
        ----------
        state_fips : int, or list of ints, optional
            state fips codes. The default is None.
        county_fips : int, or list of ints, optional
            county fips codes. The default is None.
        zipcode : int, or list of ints, optional
            zip codes. The default is None.
        cip_level : int or string, optional
            desired cip level for output table
            options {
                6 : 6 digit cip codes,
                4 : 4 digit cip codes,
                2 : 2 digit cip codes,
                all : all cip codes,
                total : total awards
                }
            The default is 6.
        cipcode : list of string or int, optional
            list containing cip codes to be subset when querying data. The default is [], meaning all.
        majornum : int, optional
            first or second major. The default is 1.
        unitid : list, optional
            list of desired unitids. The default is [].
        how : string, optional
            how would you like the data tabulated    
            options {
                total : total awards by CIP, 
                race : awards by race and CIP, 
                sex : awards by sex and CIP, 
                race_sex : awards by race, sex, and CIP
                }
            The default is 'total'.
        rename : bool, optional
            rename columns to user-friendly output. The default is True.
        label : bool, optional
            add cip code lables. The default is False.
        keep_geography : str, optional
            keep geographic detail when aggregating
        clean_geography : bool, optional
            decode geography to human-readible sentences. The default is False

        Returns
        -------
        awd : dataframe
            dataframe containing aggregated cipcode level data.

        '''
        
        awd = self.school_query(
            state_fips=state_fips,
            county_fips=county_fips,
            zipcode=zipcode,
            cip_level=cip_level,
            cipcode=cipcode,
            majornum=majornum,
            unitid=unitid,
            how=how,
            rename=rename,
            label=label,
            keep_geography=keep_geography,
            clean_geography=clean_geography).drop('UNITID',axis=1)
        
        if keep_geography == None:
            awd = awd.groupby('CIPCODE').sum().reset_index()
        else:
            awd = awd.groupby([keep_geography,'CIPCODE']).sum().reset_index()

        if label:
            awd['CIPNAME'] = awd['CIPCODE'].replace(self.metadata['CIPCODE'])
        
        if cip_level==6:
            return(self.clean(awd[[len(i)==7 for i in awd['CIPCODE']]],'C2019_A',rename=rename))
        elif cip_level==4:
            return(self.clean(awd[[len(i)==5 for i in awd['CIPCODE']]],'C2019_A',rename=rename))
        elif cip_level==2:
            return(self.clean(awd[[len(i)==2 for i in awd['CIPCODE']]],'C2019_A',rename=rename))
        elif cip_level=='all':
            return(self.clean(awd,'C2019_A',rename=rename))
        elif cip_level=='total':
            return(self.clean(awd[awd['CIPCODE']=='99'],'C2019_A',rename=rename))
        else:
            print('{} not a valid option for "cip_level". valid options: 6,4,2,all,total')
            return(None)

    def programs(self,state_fips=None,county_fips=None,zipcode=None,cip_level=6,
               cipcode=[],unitid=[],rename=True,label=False,keep_geography=None,clean_geography=False):
        '''
        generate a table for programs by cip code

        Parameters
        ----------
        state_fips : int, or list of ints, optional
            state fips codes. The default is None.
        county_fips : int, or list of ints, optional
            county fips codes. The default is None.
        zipcode : int, or list of ints, optional
            zip codes. The default is None.
        cip_level : int or string, optional
            desired cip level for output table
            options {
                6 : 6 digit cip codes,
                4 : 4 digit cip codes,
                2 : 2 digit cip codes,
                all : all cip codes,
                total : total awards
                }
            The default is 6.
        cipcode : list of string or int, optional
            list containing cip codes to be subset when querying data. The default is [], meaning all.
        unitid : list, optional
            list of desired unitids. The default is [].
        rename : bool, optional
            rename columns to user-friendly output. The default is True.
        label : bool, optional
            add cip code lables. The default is False.
        keep_geography : str, optional
            keep geographic detail when aggregating
        clean_geography : bool, optional
            decode geography to human-readible sentences. The default is False

        Returns
        -------
        prog : dataframe
            dataframe containing aggregated cipcode level data.

        '''
        
        prog = self.school_query(
            state_fips=state_fips,
            county_fips=county_fips,
            zipcode=zipcode,
            cip_level=cip_level,
            cipcode=cipcode,
            unitid=unitid,
            rename=rename,
            label=label,
            keep_geography=keep_geography,
            clean_geography=clean_geography)
        
        if keep_geography == None:
            prog = pd.DataFrame(prog.groupby('CIPCODE')['UNITID'].count()).reset_index().rename(columns={'UNITID':'PROG_COUNT'})
        else:
            prog = pd.DataFrame(prog.groupby([keep_geography,'CIPCODE'])['UNITID'].count()).reset_index().rename(columns={'UNITID':'PROG_COUNT'})

        if label:
            prog['CIPNAME'] = prog['CIPCODE'].replace(self.metadata['CIPCODE'])
        
        if cip_level==6:
            return(prog[[len(i)==7 for i in prog['CIPCODE']]])
        elif cip_level==4:
            return(prog[[len(i)==5 for i in prog['CIPCODE']]])
        elif cip_level==2:
            return(prog[[len(i)==2 for i in prog['CIPCODE']]])
        elif cip_level=='all':
            return(prog)
        elif cip_level=='total':
            return(prog[prog['CIPCODE']=='99'])
        else:
            print('{} not a valid option for "cip_level". valid options: 6,4,2,all,total')
            return(None)
               
    def schools(self,state_fips=None,county_fips=None,zipcode=None,
               unitid=[],keep_geography=None,clean_geography=False):
        '''
        generate a table for programs by cip code

        Parameters
        ----------
        state_fips : int, or list of ints, optional
            state fips codes. The default is None.
        county_fips : int, or list of ints, optional
            county fips codes. The default is None.
        zipcode : int, or list of ints, optional
            zip codes. The default is None.
        cipcode : list of string or int, optional
            list containing cip codes to be subset when querying data. The default is [], meaning all.
        unitid : list, optional
            list of desired unitids. The default is [].
        keep_geography : str, optional
            keep geographic detail when aggregating
        clean_geography : bool, optional
            decode geography to human-readible sentences. The default is False

        Returns
        -------
        schools : int or dataframe
            count of schools in region.

        '''
        
        schools = self.school_query(
            state_fips=state_fips,
            county_fips=county_fips,
            zipcode=zipcode,
            cip_level='total',
            unitid=unitid,
            keep_geography=keep_geography,
            clean_geography=clean_geography
            )
        
        if keep_geography == None:        
            try:
                return(schools['UNITID'].size)
            except Exception as e:
                print(e)
                return(None)
        
        schools = pd.DataFrame(schools.groupby(keep_geography)['UNITID'].count()).reset_index()
        return(schools)

class ONETHandler():
    target = 'D:/data/ONET_database'
    current = None
    version = 0
    current_path = None
    
    index = {
        'ab_to_wa':'Abilities to Work Activities.txt',
        'ab_to_wc':'Abilities to Work Context.txt',
        'ab':'Abilities.txt',
        'alt_title':'Alternate Titles.txt',
        'career_change':'Career Changers Matrix.txt',
        'career_start':'Career Starters Matrix.txt',
        'cont_ref':'Content Model Reference.txt',
        'dwa_ref':'DWA Reference.txt',
        'ed_cat':'Education, Training, and Experience Categories.txt',
        'ed':'Education, Training, and Experience.txt',
        'et':'Emerging Tasks.txt',
        'in':'Interests.txt',
        'iwa_ref':'IWA Reference.txt',
        'jz_ref':'Job Zone Reference.txt',
        'jz':'Job Zones.txt',
        'kn':'Knowledge.txt',
        'lsa':'Level Scale Anchors.txt',
        'occ':'Occupation Data.txt',
        'occ_level':'Occupation Level Metadata.txt',
        'sample_title':'Sample of Reported Titles.txt',
        'sc_ref':'Scales Reference.txt',
        'sk_to_wa':'Skills to Work Activities.txt',
        'sk_to_wc':'Skills to Work Context.txt',
        'sk':'Skills.txt',
        'survey_loc':'Survey Booklet Locations.txt',
        'tsk_cat':'Task Categories.txt',
        'tsk_rate':'Task Ratings.txt',
        'tsk_state':'Task Statements.txt',
        'tsk_to_dwa':'Tasks to DWAs.txt',
        'tech':'Technology Skills.txt',
        'tool':'Tools Used.txt',
        'unspsc':'UNSPSC Reference.txt',
        'wa':'Work Activities.txt',
        'wc_cat':'Work Context Categories.txt',
        'wc':'Work Context.txt',
        'ws':'Work Styles.txt',
        'wv':'Work Values.txt'
        }
    
    def __init__(self, update=False):
        '''
        Parameters
        ----------
        update : bool, optional
            check for database update. The default is False.

        Returns
        -------
        None.

        '''
        
        if not os.path.exists(self.target):
            os.makedirs(self.target)
            update = True
        elif len([i for i in os.listdir(self.target) if '.zip' not in i]) > 0:
            self.current = max([i for i in os.listdir(self.target) if '.zip' not in i])
            self.current_path = '{}/{}'.format(self.target,self.current)
            self.version = float(self.current.split('db_')[1].split('_text')[0].replace('_','.'))
        else:
            update = True
            
        if update:
            self.get_new_database()
            
    def download_data(self, link):
        '''
        download specific version of database

        Parameters
        ----------
        link : str
            url for desired version of database

        Returns
        -------
        None.

        '''
        
        file_name = '{}/{}'.format(self.target,link.split('/')[-1])
        
        try:
            r = requests.get(link, stream=True)
            if(r.status_code == requests.codes.ok):
                with open(file_name,"wb") as fd:
                    for chunk in r.iter_content(chunk_size=1024):
                        if chunk:
                            fd.write(chunk)
                    fd.close()
            with zipfile.ZipFile(file_name,'r') as zip_ref:
                zip_ref.extractall(self.target)
        except Exception as e:
            print(e)
            return()
            
    def get_new_database(self):
        '''
        checks for update to database on ONET and downloads if appropriate

        Returns
        -------
        None.

        '''
        
        url = 'https://www.onetcenter.org'
        r = requests.get('{}/db_releases.html'.format(url))
        soup = BeautifulSoup(r.content,'html.parser')
        
        #check current version
        new_version = float(soup.find('h2').text.split('O*NET ')[1].split()[0])
        if self.version >= new_version:
            print('database up to date, version {}'.format(self.current))
            return()
    
        file = '{}{}'.format(url,[i for i in soup.find_all('a') if 'text.zip' in i.get('href')][0].get('href'))
        
        self.donwload_data(file)
        
        self.current = max([i for i in os.listdir(self.target) if '.zip' not in i])
        self.version = new_version
        self.current_path = file.split('/')[-1].strip('.zip')
    
    def get_table(self, table, soc=[]):
        '''
        read in table from file

        Parameters
        ----------
        table : str
            shorthand for table name, see self.index.
        soc : list of str, optional
            list containing one or more ONET SOC codes to subset. The default is [].

        Returns
        -------
        tab : dataframe
            dataframe containing requested table data

        '''
        
        try:
            name = self.index[table]
        except Exception as e:
            print(e)
            print('{} not valid table name\nvalid table name shortand:\n{}'.format(table,self.index))
            return(None)
        
        tab = pd.read_csv('{}/{}'.format(self.current_path,name),sep='\t')
        
        if soc != []:
            tab = tab[tab['O*NET-SOC Code'].isin(soc)]
        
        return(tab)
    
    def quant_view(self, table, soc=[], scale='product', how='long'):
        '''
        produces different variations on quantiative tables
        quantitative tables must have a data value and a scale ID with values
        IM and LV

        Parameters
        ----------
        table : str
            shorthand for table name, see self.index.
        soc : list of str, optional
            list containing one or more ONET SOC codes to subset. The default is [].
        scale : str, optional
            which scale id will be used
            options {
                IM,
                LV,
                product
                }
            The default is 'product'.
        how : str, optional
            how will the final output be displayed. 
            options {
                long,
                wide_raw,
                wide_clean
                }
            The default is 'long'.

        Returns
        -------
        quant : dataframe
            dataframe containing requested quantitative information

        '''
        
        try:
            quant = self.get_table(table,soc=soc)
            
            if scale != 'product':
                if scale not in ['IM','LV']:
                    print('{} not a valid scale. options: IM, LV, product'.format(scale))
                    return(None)
                
                quant = quant[quant['Scale ID']==scale]
            else:
                quant = pd.DataFrame(quant.groupby(['O*NET-SOC Code', 
                                                    'Element ID',
                                                    'Element Name'])['Data Value'].prod()).reset_index()
            
            if how == 'long':
                return(quant[['O*NET-SOC Code','Element ID','Element Name','Data Value']])
            elif how == 'wide_clean':
                return(quant.pivot(index='O*NET-SOC Code',columns='Element Name',values='Data Value'))
            elif how == 'wide_raw':
                return(quant.pivot(index='O*NET-SOC Code',columns='Element ID',values='Data Value'))
            else:
                print('{} not a valid how. options: long, wide_clean, wide_raw'.format(how))
                return(None)
        except Exception:
            traceback.print_exc()
            print('{} not viable for quant_view'.format(table))
            return(None)
    
    def qual_view(self, table, soc=[], how='long', pivot_col=None):
        '''
        produces qualitative data tabulation. can be tabulated long or wide.
        if tabulated wide, must select a data column to use in wide pivot
        transformation.

        Parameters
        ----------
        table : str
            shorthand for table name, see self.index.
        soc : list of str, optional
            list containing one or more ONET SOC codes to subset. The default is [].
        how : str, optional
            how will the final output be displayed. 
            options {
                long,
                wide
                }
            The default is 'long'.
        pivot_col : str, optional
            name of column to pivot data on. The default is None.

        Returns
        -------
        qual : dataframe
            dataframe containing requested qualitative information

        '''
        qual = self.get_table(table,soc=soc)
        
        if how == 'long':
            return(qual)
        elif how == 'wide':
            if pivot_col == None:
                print('select categorical column to use as new columns in wide transformation')
                return(None)
            qual['bool'] = True
            qual = qual.pivot(index='O*NET-SOC Code',columns=pivot_col,values='bool').reset_index().fillna(False)
            return(qual)
        else:
            print('{} not a valid how. options: long, wide'.format(how))
            return(None)
        
class Rosetta():
    stone = None
    
    def __init__(self, path='D:/data/rosetta_stone.csv'):
        '''
        Parameters
        ----------
        path : string, optional
            path to rosetta stone file. The default is 'D:/data/rosetta_stone.csv'.

        Returns
        -------
        None.

        '''
        
        try:
            self.stone = pd.read_csv(path)
        except Exception as e:
            print(e)
            
    def translate(self, data, left, right, data_2=[], how='inner'):
        '''
        merges data with rosetta stone, adding desired column

        Parameters
        ----------
        data : dataframe
            dataframe containing input data.
        left : string
            name of column to use in merge.
        right : string
            name of column to be added from rosetta stone.
        data_2 :dataframe, optional
            dataframe to be merged to data using rosetta stone. The default is [].
        how : string, optional
            merge type. The default is 'inner'.

        Returns
        -------
        data : dataframe
            dataframe containing output data

        '''
        
        temp = self.stone[[left,right]].drop_duplicates().dropna()
        
        if (left == 'cip_2020') or (right == 'cip_2020'):
            other = left if left != 'cip_2020' else right
        
            h = []
            for index, row in temp.iterrows():
                cips = ast.literal_eval(row['cip_2020'])
                for c in cips:
                    h.append({other:row[other],
                              'cip_2020':c})
            
            temp = pd.DataFrame(h)
            
        data = data.merge(temp, on=left, how=how)
        
        if len(data_2) > 0:
            data = data.merge(data_2, on=right, how=how)
        
        return(data)
            
    
    
    
        
#TODO ACS, rosetta stone, etpl, qcew, to_excel and to_db methods








