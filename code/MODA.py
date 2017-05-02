# -*- coding: utf-8 -*-
"""
Spyder Editor


"""

import pandas as _pd
from nyc_geoclient import Geoclient
import sys as _sys
import numpy as _np
import cx_Oracle as _cx


__all__ = ('StaticData', 'DataBridge', 'GeoHelper')

class StaticData:

    def __init__(self):
        print " Static Data is pulled from //Chgoldfs01/FCTF/DATA DATA DATA/ by default."

    def GetPLUTO(self,path="//Chgoldfs01/FCTF/DATA DATA DATA/Pluto/nyc_pluto_16v1" , boros= ['BK','BX','Mn','QN','SI']  ):
        """ Returns PLUTO dataset as a dataframe

        Keyword arguments:
        boros --- specify boroughs to include as a list, default ['BK','BX','Mn','QN','SI']


        """

        if path == "//Chgoldfs01/FCTF/DATA DATA DATA/Pluto/nyc_pluto_16v1":
            print 'No path specified, using default path: ' + path


        PLUTO = _pd.DataFrame()

        for b in boros:
            filename = path + "/"+ b + ".csv"
            print 'reading .csv file: ' + b
            temp = _pd.read_csv(filename)

            print str(len(temp)) + ' rows in ' + b

            PLUTO = _pd.concat([PLUTO,temp],axis=0)

        print str(len(PLUTO)) + ' rows in total PLUTO data'

        return PLUTO



class DataBridge:
    _connection = None
    _cur        = None

    def __init__(self,user,pw):
        """ creates a databridge connection object using inputted user credentials

        Keyword arguments:
        user --- self-explanatory
        pw   --- self-explanatory

        """

        try:
            self._connection = _cx.connect('%s/%s@mspuvd-ctwcprshrd1.cscx.nycnet:1755/ANAPRD.nycnet' % (user, pw))  ## sub in your server/schema string here
            self._cur        = self._connection.cursor()
        except TypeError:
            print " please input databridge(username , pw) "


    def QueryDB(self,query):
        # open databridge connection
        # query
        self._cur.execute(query)
        # save query to dataframe
        df = _pd.DataFrame(self._cur.fetchall())
        df.columns = [rec[0] for rec in self._cur.description]
        return df





class GeoHelper:

    def __init__(self,app_id,key):
        # empty for now
         self._g = Geoclient(app_id,key)


    def _find_id_column(self, col_list):
        ids_by_pref = ['BIN','BUILDINGIDENTIFICATIONNUMBER','GEOCODEDBIN','BBL', 'GEOCODEDBBL','ADDRESS']
        col_map = dict(zip(map(str.upper,col_list),col_list))
        return next((col_map[i] for i in ids_by_pref if i in map(str.upper,col_list)))


    def _inferredGeocoder(self, input_str):
        '''
        Attempts to infer the format of the input provided for geocoding, either BIN, BBL, or Street Address (very rudimentary). Will return the complete geoclient object.
        '''
        input_str = str(input_str).replace('.0','')


        if input_str.isdigit():
            if len(input_str) == 10:  # BBL
                #print '10 digit number: attempting BBL'
                out = self._g.bbl(int(input_str[0]),int(input_str[1:6].lstrip("0")) , \
                            int(float(input_str[6:].lstrip("0"))))
            elif len(input_str) == 7: # BIN
                #print '7 digit number: attempting BIN'
                out = self._g.bin(input_str)
            else:
                #print 'Unrecognized number of digits, no ID possible'
                out = None
        else: # try to split addresses
            #print "Attempting to split text into house number / street name / borough."

            split = str.split(input_str)
            house_num   = split[0]
            street_name = " ".join(split[1:-1])
            boro_name   = split[-1]
            try:
                out = self._g.address(house_num, street_name, boro_name)
            except:
                print 'Format not recognized'
                out = None


        return out

    def _checkGeoclientValidity(self, geoclient_output):
        if 'returnCode1a' in geoclient_output:
            if str(geoclient_output['returnCode1a'][0]) == '0':
                return True
            elif geoclient_output['message']:
                return 'Error Code: ' + str(geoclient_output['message'])
        else:
            return 'error returned with no message'



    def _addressGeocoder(df):
        '''
        private function to make a generic call to NYC geoclientBatch.
        '''
        try:
            x = self._g.address(df[house_num],df[street],df[boro])
            BBL = x['bbl']
            BIN = x['buildingIdentificationNumber']
        except:
            e = _sys.exc_info()[0]
            BBL = ( "Error: %s" % e )
            BIN = BBL
        return BBL, BIN



    def get_BINandBBL(self,df, identifier_col = None):
        '''
        Uses DOITT's GeoClient (the web API to DCP's GeoSupport)
        via the python wrapper https://github.com/talos/nyc-geoclient
        to geocode a dataframe df with columns number, street, and boro.

        Returns the dataframe df with two additional columns: geocodedBBL and geocodedBIN
        '''

        if identifier_col:
            print 'using provided ID column: ' + identifier_col
        else:
            identifier_col = self._find_id_column(df.columns)
            print 'found ID column: ' + identifier_col

        def wrapper_func(x):
            out = self._inferredGeocoder(x[identifier_col])
            log = self._checkGeoclientValidity(out)
            if log is True:
                return out['bbl'] , out['buildingIdentificationNumber']
            else:
                return log, log
        df[['geocodedBBL','geocodedBIN']] = df.apply(lambda x: wrapper_func(x),axis=1).apply(_pd.Series)
        return df

    def GetLatLong(self,df,identifier_col= None):

        if identifier_col:
            print 'using provided ID column: ' + identifier_col
        else:
            identifier_col = self._find_id_column(df.columns)
            print 'found ID column: ' + identifier_col

        df['Latitude']  = _np.nan
        df['Longitude'] = _np.nan

        def wrapper_func(x):
            out = self._inferredGeocoder(x[identifier_col])
            log = self._checkGeoclientValidity(out)
            if log is True:
                return out['latitudeInternalLabel'] , out['longitudeInternalLabel']
            else:
                return log, log

        df[['Latitude','Longitude']] = df.apply(lambda x: wrapper_func(x),axis=1).apply(_pd.Series)
        return df
